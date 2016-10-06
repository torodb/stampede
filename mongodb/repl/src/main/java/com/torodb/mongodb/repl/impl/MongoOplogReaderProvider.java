
package com.torodb.mongodb.repl.impl;

import javax.inject.Inject;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoClientFactory;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.OplogReader;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;

/**
 *
 */
public class MongoOplogReaderProvider implements OplogReaderProvider {
    private final MongoClientFactory mongoClientFactory;
    
    @Inject
    public MongoOplogReaderProvider(MongoClientFactory mongoClientProvider) {
        this.mongoClientFactory = mongoClientProvider;
    }


    @Override
    public OplogReader newReader(HostAndPort syncSource) throws
            NoSyncSourceFoundException, UnreachableMongoServerException {

        MongoClient client = null;
        try {
            client = mongoClientFactory.createClient(syncSource);
            return new ClientOwnerMongoOplogReader(client);
        } catch (Throwable ex) {
            if (client != null) {
                client.close();
            }
            throw ex;
        }
    }

    @Override
    public OplogReader newReader(MongoConnection connection) {
        return new ReusedConnectionOplogReader(connection);
    }


}
