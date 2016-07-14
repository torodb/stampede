
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;

/**
 *
 */
public interface MongoClientProvider {

    public MongoClient getClient(HostAndPort hostAndPort) throws UnreachableMongoServerException;

}
