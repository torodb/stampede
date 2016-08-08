
package com.torodb.mongodb.repl.impl;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.torodb.common.util.ThreadFactoryRunnableService;
import com.torodb.core.annotations.ToroDbRunnableService;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteTransaction;
import com.torodb.mongodb.repl.OplogOperationApplier;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
class ReplSyncApplier extends ThreadFactoryRunnableService {

    private static final Logger LOGGER = LogManager.getLogger(ReplSyncApplier.class);
    private final SyncServiceView callback;
    private final OplogOperationApplier oplogOpApplier;
    private final OplogManager oplogManager;
    private final MongodConnection connection;

    private volatile Thread runThread;

    ReplSyncApplier(
            @ToroDbRunnableService ThreadFactory threadFactory,
            @Nonnull OplogOperationApplier oplogOpApplier,
            @Nonnull MongodServer server,
            @Nonnull OplogManager oplogManager,
            @Nonnull SyncServiceView callback) {
        super(threadFactory);
        this.callback = callback;
        this.connection = server.openConnection();
        this.oplogOpApplier = oplogOpApplier;
        this.oplogManager = oplogManager;
    }

    @Override
    protected String serviceName() {
        return "ToroDB Sync Applier";
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
                        LOGGER.trace("Executing {}", opToApply);
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
