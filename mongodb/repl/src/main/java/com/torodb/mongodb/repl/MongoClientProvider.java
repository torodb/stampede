
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;

/**
 *
 */
public interface MongoClientProvider {

    public MongoClient getClient(MongoClientConfiguration mongoClientConfiguration) throws UnreachableMongoServerException;

}
