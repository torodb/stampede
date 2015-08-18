
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
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

    ReplSyncApplier(
            @Nonnull Executor executor,
            @Nonnull SyncServiceView callback) {
        this.callback = callback;
    }

    @Override
    protected void run() {
        while (isRunning()) {
            try {
                OplogOperation operationToApply = callback.takeOp();

                LOGGER.info("Oplog Op consumed: " + operationToApply.toString());

                callback.markAsApplied(operationToApply);
            } catch (InterruptedException ex) {
                LOGGER.debug("Interrupted applier thread while waiting for an operator");
            }
        }
    }


    public static interface SyncServiceView {

        public OplogOperation takeOp() throws InterruptedException;

        public void markAsApplied(OplogOperation oplogOperation);

    }
}
