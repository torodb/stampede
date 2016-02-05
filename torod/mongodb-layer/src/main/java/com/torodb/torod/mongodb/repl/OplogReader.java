
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.pojos.MongoCursor;
import com.google.common.net.HostAndPort;
import java.io.Closeable;
import javax.annotation.Nonnull;

/**
 *
 */
public interface OplogReader extends Closeable {

    public HostAndPort getSyncSource();

    /**
     *
     * @param lastFetchedOpTime
     * @return a cursor that iterates over the oplog entries of the sync source
     *         whose optime is equal or higher than the given one.
     * @throws com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException
     */
    public MongoCursor<OplogOperation> queryGTE(OpTime lastFetchedOpTime) throws MongoException;

    /**
     *
     * @return the last operation applied by the sync source
     * @throws OplogStartMissingException if there no operation stored on the sync source
     * @throws OplogOperationUnsupported
     * @throws MongoException
     */
    @Nonnull
    public OplogOperation getLastOp() throws OplogStartMissingException, OplogOperationUnsupported, MongoException;

    public OplogOperation getFirstOp() throws OplogStartMissingException, OplogOperationUnsupported, MongoException;

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

    public boolean isClosed();

    /**
     * Returns a cursor that iterates throw all oplog operations on the remote
     * oplog whose optime between <em>from</em> and <em>to</em>.
     * @param from
     * @param includeFrom true iff the oplog whose optime is <em>from</em> must be returned
     * @param to
     * @param includeTo true iff the oplog whose optime is <em>to</em> must be returned
     * @return
     * @throws OplogStartMissingException
     * @throws OplogOperationUnsupported
     * @throws MongoException
     */
    public MongoCursor<OplogOperation> between(OpTime from, boolean includeFrom, OpTime to, boolean includeTo)
            throws OplogStartMissingException, OplogOperationUnsupported, MongoException;
}
