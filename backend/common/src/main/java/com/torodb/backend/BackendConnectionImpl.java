
package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.ReadOnlyBackendTransaction;
import com.torodb.core.backend.WriteBackendTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendConnectionImpl implements BackendConnection {

    private static final Logger LOGGER = LogManager.getLogger(BackendConnectionImpl.class);
    private final BackendImpl backend;
    private final SqlInterface sqlInterface;
    private boolean closed = false;
    private BackendTransaction currentTransaction;

    public BackendConnectionImpl(BackendImpl backend, SqlInterface sqlInterface) {
        this.backend = backend;
        this.sqlInterface = sqlInterface;
    }

    @Override
    public ReadOnlyBackendTransaction openReadOnlyTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);
        
        ReadOnlyBackendTransactionImpl transaction = new ReadOnlyBackendTransactionImpl(this);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public WriteBackendTransaction openWriteTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        WriteBackendTransactionImpl transaction = new WriteBackendTransactionImpl(sqlInterface, this);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (currentTransaction != null) {
                currentTransaction.close();
            }
            assert currentTransaction == null;
            backend.onConnectionClosed(this);
        }
    }

    void onTransactionClosed(BackendTransaction transaction) {
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

}
