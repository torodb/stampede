
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
import javax.annotation.Nonnull;

/**
 *
 */
public interface OplogReaderProvider {

    /**
     * Returns new oplog reader.
     *
     * The created reader uses the given host and port as sync source
     * @param syncSource
     * @return
     * @throws NoSyncSourceFoundException
     * @throws UnreachableMongoServerException
     */
    @Nonnull
    public OplogReader newReader(@Nonnull HostAndPort syncSource)
            throws NoSyncSourceFoundException, UnreachableMongoServerException;

    public OplogReader newReader(@Nonnull MongoConnection connection);
}
