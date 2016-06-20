
package com.torodb.backend;

import com.google.inject.Inject;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;

/**
 *
 */
public class BackendImpl implements Backend {

    private final SqlInterface sqlInterface;

    @Inject
    public BackendImpl(SqlInterface sqlInterface) {
        this.sqlInterface = sqlInterface;
    }

    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface);
    }

    void onConnectionClosed(BackendConnectionImpl connection) {
    }

}
