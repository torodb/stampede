
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteTransaction;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.torodb.mongodb.guice.MongoDbLayer;

/**
 *
 */
class ReplSyncApplier extends AbstractExecutionThreadService{

    private static final Logger LOGGER = LogManager.getLogger(ReplSyncApplier.class);
    private final SyncServiceView callback;
    private final Executor executor;
    private final OplogOperationApplier oplogOpApplier;
    private final OplogManager oplogManager;
    private final MongodConnection connection;

    private volatile Thread runThread;

    ReplSyncApplier(
            @MongoDbLayer @Nonnull Executor executor,
            @Nonnull OplogOperationApplier oplogOpApplier,
            @Nonnull MongodServer server,
            @Nonnull OplogManager oplogManager,
            @Nonnull SyncServiceView callback) {
        this.callback = callback;
        this.executor = executor;
        this.connection = server.openConnection();
        this.oplogOpApplier = oplogOpApplier;
        this.oplogManager = oplogManager;
    }

    @Override
    protected String serviceName() {
        return "ToroDB Sync Applier";
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void triggerShutdown() {
        if (runThread != null) {
            runThread.interrupt();
        }
    }

    @Override
    protected void run() {
        runThread = Thread.currentThread();
        while (isRunning()) {
            try (WriteTransaction oplogTransaction = oplogManager.createWriteTransaction();
                    WriteMongodTransaction transaction = connection.openWriteTransaction()) {
                try {
                    for (OplogOperation opToApply : callback.takeOps()) {
                        LOGGER.info("Executing {}", opToApply);
                        try {
                            boolean done = false;
                            while (!done) {
                                try {
                                    Status<?> status = oplogOpApplier.apply(
                                            opToApply,
                                            transaction,
                                            oplogTransaction,
                                            false
                                    );
                                    if (!status.isOK()) {
                                        if (!callback.failedToApply(null, status)) {
                                            LOGGER.error("{} stopped because one operation cannot be "
                                                    + "executed: {}", serviceName(), status);
                                            break;
                                        }
                                    }
                                    transaction.commit();
                                    done = true;
                                } catch (RollbackException ex) {
                                    LOGGER.debug("Recived a rollback exception while applying an oplog op", ex);
                                }
                            }
                        } catch (OplogManagerPersistException ex) {
                            if (callback.failedToApply(opToApply, ex)) {
                                LOGGER.error(serviceName() + " stopped because "
                                        + "the last applied operation couldn't "
                                        + "be persisted", ex);
                                break;
                            }
                        } catch (UserException ex) {
                            if (callback.failedToApply(opToApply, ex)) {

                                    LOGGER.error(serviceName() + " stopped because one operation "
                                            + "cannot be executed", ex);
                                break;
                            }
                        } catch (Throwable ex) {
                            if (callback.failedToApply(opToApply, ex)) {
                                LOGGER.error(serviceName() + " stopped because "
                                        + "an unknown error", ex);
                                break;
                            }
                        }
                        callback.markAsApplied(opToApply);
                    }
                } catch (InterruptedException ex) {
                    LOGGER.debug("Interrupted applier thread while waiting for an operator");
                }
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        connection.close();
    }


    public static interface SyncServiceView {

        public List<OplogOperation> takeOps() throws InterruptedException;

        public void markAsApplied(OplogOperation oplogOperation);

        /**
         *
         * @param oplogOperation
         * @param status
         * @return false iff the applier loop should stop
         */
        public boolean failedToApply(OplogOperation oplogOperation, Status<?> status);
        /**
         *
         * @param oplogOperation
         * @param t
         * @return false iff the applier loop should stop
         */
        public boolean failedToApply(OplogOperation oplogOperation, OplogManagerPersistException t);

        /**
         *
         * @param oplogOperation
         * @param t
         * @return false iff the applier loop should stop
         */
        public boolean failedToApply(OplogOperation oplogOperation, Throwable t);

    }
}
