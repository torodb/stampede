
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;

/**
 *
 */
public interface MongoClientProvider {

    public MongoClient getClient(HostAndPort hostAndPort, MongoClientOptions mongoClientOptions, MongoCredential mongoCredential) throws UnreachableMongoServerException;

}
