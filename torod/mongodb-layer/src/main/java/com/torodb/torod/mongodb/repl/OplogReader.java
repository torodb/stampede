
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.torodb.torod.mongodb.repl.exceptions.EmptyOplogException;
import com.torodb.torod.mongodb.repl.exceptions.InvalidOplogOperation;
import java.io.Closeable;
import javax.annotation.Nullable;

/**
 *
 */
public interface OplogReader extends Closeable {

    /**
     *
     * @param lastFetchedOpTime
     * @return false iff there is no sync source we can reply from using the given optime
     */
    public boolean connect(OpTime lastFetchedOpTime);

    /**
     *
     * @param lastFetchedOpTime
     * @return a cursor that iterates over the oplog entries of the sync source
     *         whose optime is equal or higher than the given one.
     */
    public OplogCursor queryGTE(OpTime lastFetchedOpTime);

    /**
     *
     * @return the last operation applied by the sync source
     * @throws EmptyOplogException if there no operation stored on the sync source
     * @throws com.torodb.torod.mongodb.repl.exceptions.InvalidOplogOperation
     */
    public OplogOperation getLastOp() throws EmptyOplogException, InvalidOplogOperation;

    public OplogOperation getFirstOp() throws EmptyOplogException, InvalidOplogOperation;

    /**
     *
     * @return true iff it is considered that is better to change the sync
     *         source we are using
     */
    public boolean shouldChangeSyncSource();

    /**
     * Close the reader and all resources associated with him.
     */
    @Override
    public void close();

    public static interface OplogCursor extends Closeable {

        public boolean batchIsEmpty();

        /**
         * Get, but do not consumes, the next element in the batch
         * @return
         * @throws InvalidOplogOperation
         */
        @Nullable
        public OplogOperation nextInBatch() throws InvalidOplogOperation;

        /**
         * Get and consumes the next element in the batch
         * @return
         * @throws InvalidOplogOperation
         */
        @Nullable
        public OplogOperation consumeNextInBatch() throws InvalidOplogOperation;

        /**
         *
         * @return the number of elements in the batch fetched from the sync source
         */
        public int getCurrentBatchedSize();

        public long getCurrentBatchTime();

        public void newBatch(long maxWaitTime);

        public boolean isDead();

        @Override
        public void close();
    }
}
