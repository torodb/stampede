
package com.torodb.torod.impl.sql;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.BackendConnection;
import com.torodb.torod.TorodConnection;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@NotThreadSafe
public class SqlTorodConnection implements TorodConnection {
    private static final Logger LOGGER = LogManager.getLogger(SqlTorodConnection.class);

    private final SqlTorodServer server;
    private final int connectionid;
    private final BackendConnection backendConnection;
    private boolean closed = false;
    private SqlTorodTransaction<?> currentTransaction = null;

    SqlTorodConnection(SqlTorodServer server, int connectionId) {
        this.server = server;
        this.connectionid = connectionId;
        this.backendConnection = server.getBackend().openConnection();
    }

    @Override
    public SqlReadOnlyTorodTransaction openReadOnlyTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        SqlReadOnlyTorodTransaction transaction = new SqlReadOnlyTorodTransaction(this);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public SqlSharedWriteTorodTransaction openWriteTransaction(boolean concurrent) {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        SqlSharedWriteTorodTransaction transaction = new SqlSharedWriteTorodTransaction(this, concurrent);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public SqlExclusiveWriteTorodTransaction openExclusiveWriteTransaction(boolean concurrent) {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        SqlExclusiveWriteTorodTransaction transaction = new SqlExclusiveWriteTorodTransaction(this, concurrent);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
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

    void onTransactionClosed(SqlTorodTransaction<?> transaction) {
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
    public SqlTorodServer getServer() {
        return server;
    }

    BackendConnection getBackendConnection() {
        return backendConnection;
    }

}
