
package com.torodb.torod;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.BackendConnection;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@NotThreadSafe
public class TorodConnection implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(TorodConnection.class);

    private final TorodServer server;
    private final int connectionid;
    private final BackendConnection backendConnection;
    private boolean closed = false;
    private TorodTransaction currentTransaction = null;

    TorodConnection(TorodServer server, int connectionId) {
        this.server = server;
        this.connectionid = connectionId;
        this.backendConnection = server.getBackend().openConnection();
    }

    public ReadOnlyTorodTransaction openReadOnlyTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        ReadOnlyTorodTransaction transaction = new ReadOnlyTorodTransaction(this);
        currentTransaction = transaction;

        return transaction;
    }

    public WriteTorodTransaction openWriteTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        WriteTorodTransaction transaction = new WriteTorodTransaction(this);
        currentTransaction = transaction;

        return transaction;
    }

    public int getConnectionId() {
        return connectionid;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (currentTransaction != null) {
                currentTransaction.close();
            }
            assert currentTransaction == null;
            server.onConnectionClosed(this);

            backendConnection.close();
        }
    }

    void onTransactionClosed(TorodTransaction transaction) {
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

    public TorodServer getServer() {
        return server;
    }

    BackendConnection getBackendConnection() {
        return backendConnection;
    }

    

}
