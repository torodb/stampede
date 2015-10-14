
package com.torodb.torod.mongodb.repl.impl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.torodb.torod.mongodb.repl.OplogReader;
import com.torodb.torod.mongodb.repl.OplogReaderProvider;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.torod.mongodb.utils.MongoClientProvider;
import javax.inject.Inject;

/**
 *
 */
public class MongoOplogReaderProvider implements OplogReaderProvider {
    private final MongoClientProvider mongoClientProvider;

    @Inject
    public MongoOplogReaderProvider(
            MongoClientProvider mongoClientProvider) {
        this.mongoClientProvider = mongoClientProvider;
    }

    @Override
    public OplogReader newReader(HostAndPort syncSource) throws
            NoSyncSourceFoundException, UnreachableMongoServerException {

        MongoClient client;
        client = mongoClientProvider.getClient(syncSource);
        return new ClientOwnerMongoOplogReader(client);
    }

    @Override
    public OplogReader newReader(MongoConnection connection) {
        return new ReusedConnectionOplogReader(connection);
    }


}
