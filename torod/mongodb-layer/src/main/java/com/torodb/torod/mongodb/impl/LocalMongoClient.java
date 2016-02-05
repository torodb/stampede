
package com.torodb.torod.mongodb.impl;

import com.eightkdata.mongowp.MongoVersion;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.SafeRequestProcessor;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.torodb.torod.mongodb.annotations.Local;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class LocalMongoClient implements MongoClient {

    private boolean closed;
    private final CommandsExecutor commandsExecutor;
    private final SafeRequestProcessor processor;

    @Inject
    public LocalMongoClient(
            CommandsExecutor commandsExecutor,
            @Local SafeRequestProcessor processor) {
        this.commandsExecutor = commandsExecutor;
        this.processor = processor;
        closed = false;
    }

    @Override
    public HostAndPort getAddress() {
        return null;
    }

    @Override
    public MongoVersion getMongoVersion() {
        return MongoVersion.V3_0;
    }

    @Override
    public LocalMongoConnection openConnection() {
        Preconditions.checkState(!closed, "This client is closed");
        return new LocalMongoConnection(this, processor, commandsExecutor);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

}
