
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Connection;
import com.google.common.base.Preconditions;
import com.torodb.torod.TorodConnection;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@NotThreadSafe
public class MongodConnection implements Connection, AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(MongodConnection.class);

    private final MongodServer server;
    private final TorodConnection torodConnection;
    private final LastErrorManager lastErrorManager;
    private final CommandsExecutor<? super MongodConnection> commandsExecutor;
    private MongodTransaction currentTransaction;
    private boolean closed = false;
    private final StackTraceElement[] stack;

    public MongodConnection(MongodServer server) {
        this.server = server;
        this.torodConnection = server.getTorodServer().openConnection();
        this.lastErrorManager = new LastErrorManager();
        this.commandsExecutor = server.getCommandsExecutorClassifier().getConnectionCommandsExecutor();
        stack = Thread.currentThread().getStackTrace();
    }

    public MongodServer getServer() {
        return server;
    }

    public TorodConnection getTorodConnection() {
        return torodConnection;
    }

    public LastErrorManager getLastErrorManager() {
        return lastErrorManager;
    }

    public ReadOnlyMongodTransaction openReadOnlyTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);
        ReadOnlyMongodTransaction trans = new ReadOnlyMongodTransactionImpl(this);
        currentTransaction = trans;
        return trans;
    }

    public WriteMongodTransaction openWriteTransaction() {
        return openWriteTransaction(false);
    }

    public WriteMongodTransaction openWriteTransaction(boolean concurrent) {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);
        WriteMongodTransaction trans = new WriteMongodTransactionImpl(this, concurrent);
        currentTransaction = trans;
        return trans;
    }

    @Nullable
    public MongodTransaction getCurrentTransaction() {
        return currentTransaction;
    }

    @Override
    public int getConnectionId() {
        return torodConnection.getConnectionId();
    }

    public CommandsExecutor<? super MongodConnection> getCommandsExecutor() {
        return commandsExecutor;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (currentTransaction != null) {
                currentTransaction.close();
            }
            server.onConnectionClose(this);

            torodConnection.close();
        }
    }

    void onTransactionClosed(MongodTransaction transaction) {
        if (currentTransaction == null) {
            LOGGER.debug("Recived an on transaction close notification, but there is no current transaction");
            return ;
        }
        if (currentTransaction != transaction) {
            LOGGER.debug("Recived an on transaction close notification, but the recived transaction is not the same as the current one");
            return ;
        }
        currentTransaction = null;
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            LOGGER.warn(this.getClass() + " finalized without being closed");
            close();
        }
    }

}
