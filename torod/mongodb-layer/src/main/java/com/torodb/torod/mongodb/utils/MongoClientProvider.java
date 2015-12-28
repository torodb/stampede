
package com.torodb.torod.mongodb.utils;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.google.common.net.HostAndPort;
import com.kdata.mongowp.client.wrapper.MongoClientWrapper;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;

/**
 *
 */
public class MongoClientProvider {

    public MongoClient getClient(HostAndPort hostAndPort) throws UnreachableMongoServerException {
        return new MongoClientWrapper(hostAndPort);
    }

}
