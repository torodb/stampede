
package com.torodb.torod.mongodb.repl.impl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.google.common.net.HostAndPort;

/**
 *
 */
public class ClientOwnerMongoOplogReader extends AbstractMongoOplogReader {
    private final MongoClient mongoClient;

    public ClientOwnerMongoOplogReader(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public HostAndPort getSyncSource() {
        return mongoClient.getAddress();
    }

    @Override
    protected MongoConnection consumeConnection() {
        return mongoClient.openConnection();
    }

    @Override
    protected void releaseConnection(MongoConnection connection) {
        connection.close();
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    @Override
    public boolean isClosed() {
        return mongoClient.isClosed();
    }


}
