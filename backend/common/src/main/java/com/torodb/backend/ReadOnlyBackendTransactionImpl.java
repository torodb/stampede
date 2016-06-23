
package com.torodb.backend;

import com.torodb.core.backend.ReadOnlyBackendTransaction;

/**
 *
 */
public class ReadOnlyBackendTransactionImpl implements ReadOnlyBackendTransaction {

    private boolean closed = false;
    private final BackendConnectionImpl backendConnection;

    public ReadOnlyBackendTransactionImpl(BackendConnectionImpl backendConnection) {
        this.backendConnection = backendConnection;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            backendConnection.onTransactionClosed(this);
        }
    }

}
