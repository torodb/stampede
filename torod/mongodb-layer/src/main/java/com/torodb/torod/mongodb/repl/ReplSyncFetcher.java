
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.Batch;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
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
    private final OplogReaderProvider readerProvider;
    private final Executor executor;
    private final SyncSourceProvider syncSourceProvider;
    private long opsReadCounter = 0;

    private long lastFetchedHash;
    private OpTime lastFetchedOpTime;
    private volatile Thread runThread;

    ReplSyncFetcher(
            @Nonnull Executor executor,
            @Nonnull SyncServiceView callback,
            @Nonnull SyncSourceProvider syncSourceProvider,
            @Nonnull OplogReaderProvider readerProvider,
            long lastAppliedHash,
            OpTime lastAppliedOpTime) {
        this.executor = executor;
        this.callback = callback;
        this.readerProvider = readerProvider;
        this.lastFetchedHash = 0;
        this.lastFetchedOpTime = null;
        this.syncSourceProvider = syncSourceProvider;

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
    protected void triggerShutdown() {
        if (runThread != null) {
            runThread.interrupt();
        }
    }

    @Override
    public void run() {
        runThread = Thread.currentThread();
        boolean rollbackNeeded = false;
        try {
            OplogReader oplogReader = null;
            while (!rollbackNeeded && isRunning()) {
                try {
                    if (callback.shouldPause()) {
                        callback.awaitUntilUnpaused();
                    }

                    callback.awaitUntilAllFetchedAreApplied();

                    HostAndPort syncSource = null;
                    try {
                        syncSource = syncSourceProvider.getSyncSource(lastFetchedOpTime);
                        oplogReader = readerProvider.newReader(syncSource);
                    } catch (NoSyncSourceFoundException ex) {
                        LOGGER.warn("There is no source to sync from");
                        Thread.sleep(1000);
                        continue;
                    } catch (UnreachableMongoServerException ex) {
                        assert syncSource != null;
                        LOGGER.warn("It was impossible to reach the sync source " + syncSource);
                        Thread.sleep(1000);
                        continue;
                    }

                    rollbackNeeded = fetch(oplogReader);
                } catch (InterruptedException ex) {
                    LOGGER.info("Interrupted fetch process", ex);
                } catch (RestartFetchException ex) {
                    LOGGER.info("Restarting fetch process", ex);
                } catch (Throwable ex) {
                    throw new StopFetchException(ex);
                } finally {
                    if (oplogReader != null) {
                        oplogReader.close();
                    }
                }
            }
            if (rollbackNeeded) {
                LOGGER.info("Requesting rollback");
                callback.rollback(oplogReader);
            }
            else {
                LOGGER.info(serviceName() + " ending by external request");
                callback.fetchFinished();
            }
        } catch (StopFetchException ex) {
            LOGGER.info(serviceName() + " stopped by self request");
            callback.fetchAborted(ex);
        }
        LOGGER.info(serviceName() + " stopped");
    }

    public boolean fetchIterationCanContinue() {
        return isRunning() && !callback.shouldPause();
    }

    /**
     *
     * @param reader
     * @return true iff rollback is needed
     * @throws com.torodb.torod.mongodb.repl.ReplSyncFetcher.StopFetchException
     * @throws com.torodb.torod.mongodb.repl.ReplSyncFetcher.RestartFetchException
     */
    private boolean fetch(OplogReader reader) throws StopFetchException, RestartFetchException {

        try {

            MongoCursor<OplogOperation> cursor = reader.queryGTE(lastFetchedOpTime);
            Batch<OplogOperation> batch = cursor.fetchBatch();
            postBatchChecks(reader, cursor, batch);

            try {
                if (isRollbackNeeded(reader, batch, lastFetchedOpTime, lastFetchedHash)) {
                    return true;
                }
                
                while (fetchIterationCanContinue()) {
                    if (!batch.hasNext()) {
                        preBatchChecks(batch);
                        batch = cursor.fetchBatch();
                        postBatchChecks(reader, cursor, batch);
                        continue;
                    }

                    if (batch.hasNext()) {
                        OplogOperation nextOp = batch.next();
                        assert nextOp != null;
                        boolean delivered = false;
                        while (!delivered) {
                            try {
                                LOGGER.debug("Delivered op: {}", nextOp);
                                callback.deliver(nextOp);
                                delivered = true;
                                opsReadCounter++;
                            } catch (InterruptedException ex) {
                                LOGGER.warn(serviceName() + " interrupted while a "
                                        + "message was being to deliver. Retrying", ex);
                            }
                        }

                        lastFetchedHash = nextOp.getHash();
                        lastFetchedOpTime = nextOp.getOpTime();
                    }
                }
            } finally {
                cursor.close();
            }

        } catch (MongoException ex) {
            throw new RestartFetchException();
        }
        return false;
    }

    /**
     * @param oldBatch
     */
    private void preBatchChecks(Batch<OplogOperation> oldBatch) {
        int batchSize = oldBatch.getBatchSize();
        if (batchSize > 0 && batchSize < MIN_BATCH_SIZE) {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = oldBatch.getFetchTime() - currentTime;
            if (elapsedTime < SLEEP_TO_BATCH_MILLIS) {
                try {
                    LOGGER.debug("Batch size is very small. Waiting {} millis for more...", SLEEP_TO_BATCH_MILLIS);
                    Thread.sleep(SLEEP_TO_BATCH_MILLIS);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    /**
     *
     * @param cursor
     * @throws InterruptedException
     */
    private void postBatchChecks(OplogReader reader, MongoCursor<OplogOperation> cursor, Batch<OplogOperation> newBatch)
            throws RestartFetchException {
        if (newBatch == null) {
            throw new RestartFetchException();
        }
        infrequentChecks(reader);
        
        if (!newBatch.hasNext() && cursor.isDead()) {
            throw new RestartFetchException();
        }
        //TODO: log stats
    }

    private void infrequentChecks(OplogReader reader) throws RestartFetchException {
        if (reader.shouldChangeSyncSource()) {
            LOGGER.info("A better sync source has been detected");
            throw new RestartFetchException();
        }
    }

    private boolean isRollbackNeeded(
            OplogReader reader,
            Batch<OplogOperation> batch,
            OpTime lastFetchedOpTime,
            long lastFetchedHash) throws StopFetchException {
        if (!batch.hasNext()) {
            try {
                /*
                 * our last query return an empty set. But we can still detect a
                 * rollback if the last operation stored on the sync source is
                 * before our last optime fetched
                 */
                OplogOperation lastOp = reader.getLastOp();

                if (lastOp.getOpTime().compareTo(lastFetchedOpTime) < 0) {
                    LOGGER.info("We are ahead of the sync source. Rolling back");
                    return true;
                }
            }
            catch (OplogStartMissingException ex) {
                LOGGER.error("Sync source contais no operation on his oplog!");
                throw new StopFetchException();
            } catch (OplogOperationUnsupported ex) {
                LOGGER.error("Sync source contais an invalid operation!");
                throw new StopFetchException(ex);
            } catch (MongoException ex) {
                LOGGER.error("Unknown error while trying to fetch last remote operation", ex);
                throw new StopFetchException(ex);
            }
        }
        else {
            OplogOperation firstOp = batch.next();
            assert firstOp != null;
            if (firstOp.getHash() != lastFetchedHash
                    || !firstOp.getOpTime().equals(lastFetchedOpTime)) {

                LOGGER.info(
                        "Rolling back: Our last fetched = [{}, {}]. Source = [{}, {}]",
                        lastFetchedOpTime,
                        lastFetchedHash,
                        firstOp.getOpTime(),
                        firstOp.getHash()
                );

                return true;
            }
        }
        return false;
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

        public void awaitUntilAllFetchedAreApplied();

        public void fetchFinished();

        public void fetchAborted(Throwable ex);
    }
}
