
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.google.common.net.HostAndPort;

/**
 *
 */
public interface MongoClientProvider {

    public MongoClient getClient(HostAndPort hostAndPort) throws UnreachableMongoServerException;

}
