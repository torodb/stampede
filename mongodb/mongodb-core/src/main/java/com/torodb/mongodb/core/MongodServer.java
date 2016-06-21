
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.mongodb.mongowp.ConnectionCommandsExecutor;
import com.torodb.mongodb.mongowp.ReadOnlyTransactionCommandsExecutor;
import com.torodb.mongodb.mongowp.WriteTransactionCommandsExecutor;
import com.torodb.torod.TorodServer;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class MongodServer extends AbstractIdleService {
    private final TorodServer torodServer;
    private final Cache<Integer, MongodConnection> openConnections;
    private final WriteTransactionCommandsExecutor writeCommandsExecutor;
    private final ReadOnlyTransactionCommandsExecutor readOnlyCommandsExecutor;
    private final ConnectionCommandsExecutor connectionCommandsExecutor;

    @Inject
    public MongodServer(TorodServer torodServer, 
            WriteTransactionCommandsExecutor writeCommandsExecutor,
            ReadOnlyTransactionCommandsExecutor readOnlyCommandsExecutor,
            ConnectionCommandsExecutor connectionCommandsExecutor) {
        this.torodServer = torodServer;
        openConnections = CacheBuilder.newBuilder()
                .weakValues()
                .removalListener(this::onConnectionInvalidated)
                .build();
        this.writeCommandsExecutor = writeCommandsExecutor;
        this.readOnlyCommandsExecutor = readOnlyCommandsExecutor;
        this.connectionCommandsExecutor = connectionCommandsExecutor;
    }

    public TorodServer getTorodServer() {
        return torodServer;
    }

    public MongodConnection openConnection() {
        MongodConnection connection = new MongodConnection(this);
        openConnections.put(connection.getConnectionId(), connection);

        return connection;
    }

    @Override
    protected void startUp() throws Exception {
        torodServer.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        openConnections.invalidateAll();
    }

    CommandsExecutor<WriteMongodTransaction> getWriteCommandsExecutor() {
        return writeCommandsExecutor;
    }

    CommandsExecutor<ReadOnlyMongodTransaction> getReadOnlyCommandsExecutor() {
        return readOnlyCommandsExecutor;
    }

    CommandsExecutor<MongodConnection> getConnectionCommandsExecutor() {
        return connectionCommandsExecutor;
    }

    void onConnectionClose(MongodConnection connection) {
        openConnections.invalidate(connection.getConnectionId());
    }

    private void onConnectionInvalidated(RemovalNotification<Integer, MongodConnection> notification) {
        MongodConnection value = notification.getValue();
        if (value != null) {
            value.close();
        }
    }
}