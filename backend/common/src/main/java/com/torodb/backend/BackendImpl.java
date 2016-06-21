
package com.torodb.backend;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.torodb.core.backend.Backend;
import com.torodb.core.backend.BackendConnection;

/**
 *
 */
public class BackendImpl extends AbstractIdleService implements Backend {

    private final SqlInterface sqlInterface;

    @Inject
    public BackendImpl(SqlInterface sqlInterface) {
        this.sqlInterface = sqlInterface;
    }

    @Override
    public BackendConnection openConnection() {
        return new BackendConnectionImpl(this, sqlInterface);
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

    void onConnectionClosed(BackendConnectionImpl connection) {
    }

}
