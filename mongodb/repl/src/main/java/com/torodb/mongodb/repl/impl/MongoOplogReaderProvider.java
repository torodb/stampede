
package com.torodb.mongodb.repl.impl;

import javax.inject.Inject;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.torodb.mongodb.repl.MongoClientProvider;
import com.torodb.mongodb.repl.OplogReader;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

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
    public OplogReader newReader(MongoClientConfiguration syncSource) throws
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
