
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapper;
import com.google.common.net.HostAndPort;

/**
 *
 */
public class MongoClientProvider {

    public MongoClient getClient(HostAndPort hostAndPort) throws UnreachableMongoServerException {
        return new MongoClientWrapper(hostAndPort);
    }

}
