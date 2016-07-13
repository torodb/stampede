
package com.torodb.mongodb.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.AbstractIdleService;
import com.torodb.mongodb.commands.CommandsExecutorClassifier;
import com.torodb.torod.TorodServer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class MongodServer extends AbstractIdleService {
    private static final Logger LOGGER = LogManager.getLogger(MongodServer.class);
    private final TorodServer torodServer;
    private final Cache<Integer, MongodConnection> openConnections;
    private CommandsExecutorClassifier commandsExecutorClassifier;

    @Inject
    public MongodServer(TorodServer torodServer, CommandsExecutorClassifier commandsExecutorClassifier) {
        this.torodServer = torodServer;
        openConnections = CacheBuilder.newBuilder()
                .weakValues()
                .removalListener(this::onConnectionInvalidated)
                .build();
        this.commandsExecutorClassifier = commandsExecutorClassifier;
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
        LOGGER.debug("Waiting for Torod server to be running");
        torodServer.awaitRunning();
        LOGGER.debug("MongodServer ready to run");
    }

    @Override
    protected void shutDown() throws Exception {
        openConnections.invalidateAll();
    }

    public CommandsExecutorClassifier getCommandsExecutorClassifier() {
        return commandsExecutorClassifier;
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
