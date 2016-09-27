
package com.torodb.mongodb.repl.oplogreplier.fetcher;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.Batch;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor.DeadCursorException;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.retrier.Retrier.Hint;
import com.torodb.core.retrier.RetrierAbortException;
import com.torodb.core.retrier.RetrierGiveUpException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.repl.OplogReader;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.ReplMetrics;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.oplogreplier.*;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@NotThreadSafe
public class ContinuousOplogFetcher implements OplogFetcher {

    private static final Logger LOGGER = LogManager.getLogger(ContinuousOplogFetcher.class);

    private final OplogReaderProvider readerProvider;
    private final SyncSourceProvider syncSourceProvider;
    private final Retrier retrier;
    private final FetcherState state;
    private final ReplMetrics metrics;

    @Inject
    public ContinuousOplogFetcher(OplogReaderProvider readerProvider, SyncSourceProvider syncSourceProvider,
            Retrier retrier, @Assisted long lastFetchedHash, @Assisted OpTime lastFetchedOptime, ReplMetrics metrics) {
        this.readerProvider = readerProvider;
        this.syncSourceProvider = syncSourceProvider;
        this.retrier = retrier;
        this.state = new FetcherState(lastFetchedHash, lastFetchedOptime);
        this.metrics = metrics;
    }

    public static interface ContinuousOplogFetcherFactory {
        ContinuousOplogFetcher createFetcher(long lastFetchedHash, OpTime lastFetchedOptime);
    }

    @Override
    public OplogBatch fetch() throws StopReplicationException, RollbackReplicationException {
        try {
            return retrier.retry(() -> {
                try {
                    state.prepareToFetch();

                    MongoCursor<OplogOperation> cursor = state.getLastUsedMongoCursor();
                    Batch<OplogOperation> batch = cursor.tryFetchBatch();
//                    Batch<OplogOperation> batch = cursor.fetchBatch();

                    if (batch == null || !batch.hasNext()) {
                        Thread.sleep(1000);
                        batch = cursor.tryFetchBatch();
                        if (batch == null || !batch.hasNext()) {
                            return NotReadyForMoreOplogBatch.getInstance();
                        }
                    }
                    List<OplogOperation> fetchedOps = null;
                    long fetchTime = 0;

                    /*
                     * As we already modify the cursor by fetching the batch, we cannot retry the
                     * whole block (as the cursor would be reused and the previous batch will be
                     * discarted).
                     *
                     * Then, if we leave the following try section with an error, we need to discard
                     * the cursor, so the next iteration starts from the last batch we returned. On
                     * the other hand, if we finished successfully, then we need to update the state.
                     */
                    boolean successful = false;
                    try {
                        fetchedOps = batch.asList();
                        fetchTime = batch.getFetchTime();

                        postBatchChecks(cursor, fetchedOps);

                        OplogBatch result = new NormalOplogBatch(fetchedOps, true);
                        successful = true;
                        return result;
                    } finally {
                        if (!successful) {
                            cursor.close();
                        } else {
                            assert fetchedOps != null;
                            assert fetchTime != 0;
                            state.updateState(fetchedOps, fetchTime);
                        }
                    }
                } catch (RestartFetchException ex) {
                    state.discardReader(); //lets choose a new reader
                    throw new RollbackException(ex); //and then try again
                } catch (DeadCursorException ex) {
                    throw new RollbackException(ex); //lets retry the whole block with the same reader
                } catch (StopReplicationException | RollbackReplicationException ex) { //a business error
                    throw new RetrierAbortException(ex); //do not try again
                }
            }, Hint.CRITICAL, Hint.TIME_SENSIBLE);
        } catch (RetrierGiveUpException ex) {
            this.close();
            throw new StopReplicationException("Stopping replication after several attepts to "
                    + "fetch the remote oplog", ex);
        } catch (RetrierAbortException ex) {
            this.close();
            Throwable cause = ex.getCause();
            if (cause != null) {
                if (cause instanceof StopReplicationException) {
                    throw (StopReplicationException) cause;
                }
                if (cause instanceof RollbackReplicationException) {
                    throw (RollbackReplicationException) cause;
                }
            }
            throw new StopReplicationException("Stopping replication after a unknown abort "
                    + "exception", ex);
        }
    }

    @Override
    public void close() {
        state.close();
    }

    /**
     *
     * @param cursor
     * @throws InterruptedException
     */
    private void postBatchChecks(MongoCursor<OplogOperation> cursor, @Nonnull List<OplogOperation> fetchedOps)
            throws RollbackException, RestartFetchException {
        if (fetchedOps.isEmpty()) { //TODO(gortiz): check if this is correct. At this moment I don't get why it is doing
            if (cursor.hasNext()) {
                throw new RollbackException();
            }
        }
        //TODO: log stats
    }

    private void checkRollback(OplogReader reader, @Nullable OplogOperation firstCursorOp)
            throws StopReplicationException, RollbackReplicationException {
        if (firstCursorOp == null) {
            try {
                /*
                 * our last query return an empty set. But we can still detect a
                 * rollback if the last operation stored on the sync source is
                 * before our last optime fetched
                 */
                OplogOperation lastOp = reader.getLastOp();

                if (lastOp.getOpTime().compareTo(state.lastFetchedOpTime) < 0) {
                    throw new RollbackReplicationException("We are ahead of the sync source. Rolling back");
                }
            }
            catch (OplogStartMissingException ex) {
                throw new StopReplicationException("Sync source contais no operation on his oplog!");
            } catch (OplogOperationUnsupported ex) {
                throw new StopReplicationException("Sync source contais an invalid operation!", ex);
            } catch (MongoException ex) {
                throw new StopReplicationException("Unknown error while trying to fetch last remote "
                        + "operation", ex);
            }
        }
        else {
            if (firstCursorOp.getHash() != state.lastFetchedHash
                    || !firstCursorOp.getOpTime().equals(state.lastFetchedOpTime)) {

                throw new RollbackReplicationException("Rolling back: Our last fetched = ["
                        + state.lastFetchedOpTime + ", " + state.lastFetchedHash + "]. Source = ["
                        + firstCursorOp.getOpTime() + ", " + firstCursorOp.getHash() + "]");
            }
        }
    }

    private class FetcherState implements AutoCloseable {

        private long lastFetchedHash;
        private OpTime lastFetchedOpTime;
        private OplogReader oplogReader;
        private MongoCursor<OplogOperation> cursor;

        private FetcherState(long lastFetchedHash, OpTime lastFetchedOpTime) {
            this.lastFetchedHash = lastFetchedHash;
            this.lastFetchedOpTime = lastFetchedOpTime;
        }

        private void prepareToFetch() throws StopReplicationException, RollbackException, RollbackReplicationException {
            if (oplogReader == null) {
                calculateOplogReader();
            } else if (oplogReader.shouldChangeSyncSource()) {
                LOGGER.info("A better sync source has been detected");
                discardReader();
                calculateOplogReader();
            }
            if (cursor == null || cursor.isClosed()) {
                calculateMongoCursor();
            }
        }

        @Nonnull
        private OplogReader getLastUsedOplogReader() {
            Preconditions.checkState(oplogReader != null, "The oplog reader must be calculated before");
            return oplogReader;
        }

        @Nonnull 
        private OplogReader calculateOplogReader() throws StopReplicationException {
            if (oplogReader == null) {
                try {
                    oplogReader = retrier.retry(() -> {
                        HostAndPort syncSource = syncSourceProvider.newSyncSource(lastFetchedOpTime);
                        return readerProvider.newReader(syncSource);
                    }, Hint.TIME_SENSIBLE, Hint.CRITICAL);
                } catch (RetrierGiveUpException ex) {
                    throw new StopReplicationException("It was impossible find a reachable sync source", ex);
                }
            }
            return oplogReader;
        }

        /**
         * Returns the last cursor calculated with {@link #calculateMongoCursor() } if it is closed
         * or throw an exception in other case.
         * @return
         */
        @Nonnull
        private MongoCursor<OplogOperation> getLastUsedMongoCursor() throws RestartFetchException {
            if (cursor == null || cursor.isClosed()) {
                throw new RestartFetchException("The cursor has not been calculated or it is closed");
            }
            return cursor;
        }

        /**
         * Returns a cursor that iterates on the oplog from the last fetched operation (as indicated
         * by {@link #lastFetchedOpTime} and {@link #lastFetchedHash}) (excluded).
         *
         * If there is an already open cursor, then the same cursor is returned. In other case, a
         * new cursor is created using {@link #getLastUsedOplogReader() the last used oplog reader}
         * and several checks are done to ensure that the first operation returned by this cursor is
         * the one that follows the last fetched operation.
         *
         * @return
         * @throws StopReplicationException
         * @throws RollbackException
         * @throws RollbackReplicationException
         */
        private MongoCursor<OplogOperation> calculateMongoCursor() 
                throws StopReplicationException, RollbackException, RollbackReplicationException {
            //The oplog reader could be get here, but we need to be sure that the cursor is related
            //to the reader that can be read from outside
            if (cursor == null || cursor.isClosed()) {
                try {
                    cursor = getLastUsedOplogReader().queryGTE(lastFetchedOpTime);

                    OplogOperation firstCursorOp;
                    if (cursor.hasNext()) {
                        firstCursorOp = cursor.next();
                    } else {
                        firstCursorOp = null;
                    }

                    checkRollback(oplogReader, firstCursorOp);
                } catch (MongoException ex) {
                    throw new RollbackException(ex);
                }
            }
            return cursor;
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.close();
            }
        }

        private void discardReader() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
            oplogReader = null;
        }

        private void updateState(List<OplogOperation> fetchedOps, long fetchTime) {
            int fetchedOpsSize = fetchedOps.size();

            if (fetchedOpsSize == 0) {
                return ;
            }
            
            OplogOperation lastOp = fetchedOps.get(fetchedOpsSize - 1);
            lastFetchedHash = lastOp.getHash();
            lastFetchedOpTime = lastOp.getOpTime();

            metrics.getLastOpTimeFetched().setValue(state.lastFetchedOpTime.toString());
        }

    }

    private static class RestartFetchException extends Exception {
        private static final long serialVersionUID = 1L;

        private RestartFetchException() {
        }

        private RestartFetchException(String message, RetrierGiveUpException ex) {
            super(message, ex);
        }

        private RestartFetchException(String message) {
            super(message);
        }
    }
}
