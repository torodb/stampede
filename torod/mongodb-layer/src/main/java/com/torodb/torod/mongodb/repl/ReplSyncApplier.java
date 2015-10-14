
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.torod.mongodb.impl.LocalMongoClient;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import com.torodb.torod.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.torod.mongodb.repl.OplogManager.WriteTransaction;
import com.torodb.torod.mongodb.utils.OplogOperationApplier;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class ReplSyncApplier extends AbstractExecutionThreadService{

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ReplSyncApplier.class);
    private final SyncServiceView callback;
    private final Executor executor;
    private final OplogOperationApplier oplogOpApplier;
    private final OplogManager oplogManager;
    private final LocalMongoClient localClient;

    private volatile Thread runThread;

    ReplSyncApplier(
            @Nonnull Executor executor,
            @Nonnull OplogOperationApplier oplogOpApplier,
            @Nonnull LocalMongoClient localClient,
            @Nonnull OplogManager oplogManager,
            @Nonnull SyncServiceView callback) {
        this.callback = callback;
        this.executor = executor;
        this.localClient = localClient;
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
            WriteTransaction oplogTransaction = oplogManager.createWriteTransaction();
            LocalMongoConnection localConnection = localClient.openConnection();
            try {
                try {
                    for (OplogOperation opToApply : callback.takeOps()) {
                        try {
                            //TODO: THIS IS WRONG! WE SHOULD USE UPSERT HERE!
                            //      but it is not supported yet! :(
                            oplogOpApplier.apply(
                                    opToApply,
                                    localConnection,
                                    oplogTransaction,
                                    false
                            );
                        }
                        catch (MongoException ex) {
                            if (!callback.failedToApply(null, ex)) {
                                LOGGER.error(serviceName()
                                        + " stopped because one "
                                        + "operation could not be executed", ex);
                                break;
                            }
                        } catch (OplogManagerPersistException ex) {
                            if (callback.failedToApply(opToApply, ex)) {
                                LOGGER.error(serviceName() + " stopped because "
                                        + "the last applied operation couldn't "
                                        + "be persist", ex);
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
                }
                catch (InterruptedException ex) {
                    LOGGER.debug("Interrupted applier thread while waiting for an operator");
                }
            } finally {
                oplogTransaction.close();
                localConnection.close();
            }
        }
    }


    public static interface SyncServiceView {

        public List<OplogOperation> takeOps() throws InterruptedException;

        public void markAsApplied(OplogOperation oplogOperation);

        /**
         *
         * @param oplogOperation
         * @param t
         * @return false iff the applier loop should stop
         */
        public boolean failedToApply(OplogOperation oplogOperation, MongoException t);

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
