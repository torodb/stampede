
package com.torodb.mongodb.repl;

import javax.annotation.Nonnull;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

/**
 *
 */
public interface OplogReaderProvider {

    /**
     * Returns new oplog reader.
     *
     * The created reader uses the given host and port as sync source
     * @param syncSource
     * @param mongoClientOptions
     * @param mongoCredential
     * @return
     * @throws NoSyncSourceFoundException
     * @throws UnreachableMongoServerException
     */
    @Nonnull
    public OplogReader newReader(@Nonnull HostAndPort syncSource)
            throws NoSyncSourceFoundException, UnreachableMongoServerException;

    public OplogReader newReader(@Nonnull MongoConnection connection);
}
