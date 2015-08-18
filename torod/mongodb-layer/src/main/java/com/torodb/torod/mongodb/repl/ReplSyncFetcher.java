
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.torod.mongodb.repl.exceptions.EmptyOplogException;
import com.torodb.torod.mongodb.repl.exceptions.InvalidOplogOperation;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@NotThreadSafe
class ReplSyncFetcher extends AbstractExecutionThreadService {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ReplSyncFetcher.class);
    private static final int MIN_BATCH_SIZE = 5;
    private static final long SLEEP_TO_BATCH_MILLIS = 2;
    
    private final SyncServiceView callback;
    private final OplogReader reader;
    private final Executor executor;
    private long opsReadCounter = 0;

    private long lastFetchedHash;
    private OpTime lastFetchedOpTime;

    ReplSyncFetcher(
            @Nonnull Executor executor,
            @Nonnull SyncServiceView callback,
            @Nonnull OplogReader reader,
            long lastAppliedHash,
            OpTime lastAppliedOpTime) {
        this.executor = executor;
        this.callback = callback;
        this.reader = reader;
        this.lastFetchedHash = 0;
        this.lastFetchedOpTime = null;

        this.lastFetchedHash = lastAppliedHash;
        this.lastFetchedOpTime = lastAppliedOpTime;
    }

    @Override
    protected String serviceName() {
        return "ToroDB Sync Fetcher";
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    /**
     * 
     * @return an approximation to the number of operations that has been fetched
     */
    public long getOpsReadCounter() {
        return opsReadCounter;
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                try {
                    if (callback.shouldPause()) {
                        callback.awaitUntilUnpaused();
                    }
                    
                    if (!reader.connect(lastFetchedOpTime)) {
                        LOGGER.warn("There is no source to sync from");
                        Thread.sleep(1000);
                        continue;
                    }
                    
                    fetch();
                } catch (InterruptedException ex) {
                } catch (RestartFetchException ex) {
                }
            }
            LOGGER.info(serviceName() + " ending by external request");
        }
        catch (StopFetchException ex) {
            LOGGER.info(serviceName() + " stopped by self request");
        } catch (InvalidOplogOperation ex) {
            LOGGER.error("Error while fetching an oplog", ex);
        }
        LOGGER.info(serviceName() + " stopped");
    }

    public boolean fetchIterationCanContinue() {
        return isRunning() && !callback.shouldPause();
    }

    private void fetch() throws StopFetchException, RestartFetchException, InvalidOplogOperation {

        OplogReader.OplogCursor cursor = reader.queryGTE(lastFetchedOpTime);

        try {
            tryRollback(cursor, lastFetchedOpTime, lastFetchedHash);

            while (fetchIterationCanContinue()) {
                if (readMoreIfNeeded(cursor)) {
                    if (!fetchIterationCanContinue()) {
                        continue;
                    }
                }

                if (!cursor.batchIsEmpty()) {
                    OplogOperation nextOp = cursor.consumeNextInBatch();
                    assert nextOp != null;
                    boolean delivered = false;
                    while (!delivered) {
                        try {
                            LOGGER.info("Delivered op: {}", nextOp);
                            callback.deliver(nextOp);
                            delivered = true;
                            opsReadCounter++;
                        } catch (InterruptedException ex) {
                            LOGGER.warn(serviceName() + " interrupted while a "
                                    + "message was being to deliver. Retrying", ex);
                        }
                    }

                    lastFetchedHash = nextOp.getHash();
                    lastFetchedOpTime = nextOp.getOptime();
                }
            }
        } finally {
            cursor.close();
        }

    }

    /**
     *
     * @param cursor
     * @throws InterruptedException
     * @return true iff {@linkplain #fetchIterationCanContinue() } must be called after call this method
     */
    private boolean readMoreIfNeeded(OplogReader.OplogCursor cursor) throws RestartFetchException {
        boolean isRunningMustBeCalled = false;
        if (cursor.batchIsEmpty()) {
            int batchSize = cursor.getCurrentBatchedSize();
            if (batchSize > 0 && batchSize < MIN_BATCH_SIZE) {
                long currentTime = System.currentTimeMillis();
                long elapsedTime = cursor.getCurrentBatchTime() - currentTime;
                if (elapsedTime < SLEEP_TO_BATCH_MILLIS) {
                    try {
                        LOGGER.debug("Batch size is very small. Waiting {} millis for more...", SLEEP_TO_BATCH_MILLIS);
                        Thread.sleep(SLEEP_TO_BATCH_MILLIS);
                        isRunningMustBeCalled = true;
                    } catch (InterruptedException ex) {
                    }
                }
            }

            if (!isRunningMustBeCalled && fetchIterationCanContinue()) {
                infrequentChecks(cursor);
            }

            cursor.newBatch(5000l);
            isRunningMustBeCalled = true;

            if (cursor.batchIsEmpty()) {
                if (cursor.isDead()) {
                    throw new RestartFetchException();
                }
            }
        }
        //TODO: log stats
        return isRunningMustBeCalled;
    }

    private void infrequentChecks(OplogReader.OplogCursor cursor) throws RestartFetchException {
        if (reader.shouldChangeSyncSource()) {
            LOGGER.info("A better sync source has been detected");
            throw new RestartFetchException();
        }
    }

    private void tryRollback(
            OplogReader.OplogCursor cursor,
            OpTime lastFetchedOpTime,
            long lastFetchedHash) throws StopFetchException, InvalidOplogOperation {
        if (cursor.batchIsEmpty()) {
            try {
                /*
                 * our last query return an empty set. But we can still detect a
                 * rollback if the last operation stored on the sync source is
                 * before our last optime fetched
                 */
                OplogOperation lastOp = reader.getLastOp();

                if (lastOp.getOptime().compareTo(lastFetchedOpTime) < 0) {
                    LOGGER.info("We are ahead of the sync source. Rolling back");
                    callback.rollback(reader);
                    throw new StopFetchException();
                }
            }
            catch (EmptyOplogException ex) {
                LOGGER.error("Sync source contais no operation on his oplog!");
                throw new StopFetchException();
            } catch (InvalidOplogOperation ex) {
                LOGGER.error("Sync source contais an invalid operation!");
                throw new StopFetchException(ex);
            }
        }
        else {
            OplogOperation firstOp = cursor.nextInBatch();
            assert firstOp != null;
            if (firstOp.getHash() != lastFetchedHash
                    || !firstOp.getOptime().equals(lastFetchedOpTime)) {

                LOGGER.info(
                        "Rolling back: Our last fetched = [{}, {}]. Source = [{}, {}]",
                        lastFetchedOpTime,
                        lastFetchedHash,
                        firstOp.getOptime(),
                        firstOp.getHash()
                );

                callback.rollback(reader);
                throw new StopFetchException();
            }
        }
    }

    private static class RestartFetchException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static class StopFetchException extends Exception {
        private static final long serialVersionUID = 1L;

        public StopFetchException() {
        }

        public StopFetchException(Throwable cause) {
            super(cause);
        }
    }

    public static interface SyncServiceView {

        void deliver(@Nonnull OplogOperation oplogOp) throws InterruptedException;

        void rollback(OplogReader reader);

        void awaitUntilUnpaused() throws InterruptedException;

        boolean shouldPause();
    }


}
