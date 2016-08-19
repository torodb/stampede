
package com.torodb.mongodb.repl.oplogreplier;

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
import com.torodb.mongodb.repl.OplogManager.WriteOplogTransaction;
import com.torodb.mongodb.repl.oplogreplier.OplogOperationApplier.OplogApplyingException;
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
        /*
         * TODO: In general, the replication context can be set as not reaplying. But it is not
         * frequent but possible to stop the replication after some oplog ops have been apply but
         * not marked as executed on the oplog manager.
         * For that reason, all oplog ops betwen the last operation that have been marked as
         * applyed and the current last operation on the remote oplog must be executed as replying
         * operations.
         * As it is not possible to do that yet, we have to always apply operations as replying to
         * be safe.
         */
        ApplierContext applierContext = new ApplierContext(true);
        while (isRunning()) {
            OplogOperation lastOperation = null;
            try (WriteMongodTransaction transaction = connection.openWriteTransaction()) {
                try {
                    for (OplogOperation opToApply : callback.takeOps()) {
                        lastOperation = opToApply;
                        LOGGER.trace("Executing {}", opToApply);
                        try {
                            boolean done = false;
                            while (!done) {
                                try {
                                    oplogOpApplier.apply(
                                            opToApply,
                                            transaction,
                                            applierContext
                                    );
                                    transaction.commit();
                                    done = true;
                                } catch (RollbackException ex) {
                                    LOGGER.debug("Recived a rollback exception while applying an oplog op", ex);
                                }
                            }
                        } catch (OplogApplyingException ex) {
                            if (!callback.failedToApply(opToApply, ex)) {
                                LOGGER.error(serviceName() + " stopped because one operation "
                                        + "cannot be executed", ex);
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
            if(lastOperation != null) {
                try (WriteOplogTransaction oplogTransaction = oplogManager.createWriteTransaction()) {
                    oplogTransaction.addOperation(lastOperation);
                } catch (OplogManagerPersistException ex) {
                    if (callback.failedToApply(lastOperation, ex)) {
                        LOGGER.error(serviceName() + " stopped because "
                                + "the last applied operation couldn't "
                                + "be persisted", ex);
                        break;
                    }
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
