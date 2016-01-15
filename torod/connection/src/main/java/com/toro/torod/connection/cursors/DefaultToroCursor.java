
package com.toro.torod.connection.cursors;

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.ToroCursor;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.executor.SessionExecutor;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public abstract class DefaultToroCursor implements ToroCursor {

    private final CursorId id;
    private final boolean hasTimeout;
    private final int limit;
    private final boolean autoclosable;
    private boolean closed;

    public DefaultToroCursor(CursorId id, boolean hasTimeout, int limit, boolean autoclosable) {
        this.id = id;
        this.hasTimeout = hasTimeout;
        this.limit = limit;
        this.autoclosable = autoclosable;
        this.closed = false;
    }

    public DefaultToroCursor(CursorId id, boolean hasTimeout, boolean autoclosable) {
        this.id = id;
        this.hasTimeout = hasTimeout;
        this.limit = 0;
        this.autoclosable = autoclosable;
        this.closed = false;
    }
    
    protected abstract void closeImmediately(SessionExecutor executor);
    
    @Override
    public CursorId getId() {
        return id;
    }

    @Override
    public boolean hasTimeout() {
        return hasTimeout;
    }

    @Override
    public boolean hasLimit() {
        return limit > 1;
    }

    @Override
    public int getLimit() throws IllegalStateException {
        if (!hasLimit()) {
            throw new IllegalStateException("This cursor doesn't have a limit, so this method cannot be called");
        }
        return limit;
    }

    @Override
    public boolean isAutoclosable() {
        return autoclosable;
    }

    @Override
    public synchronized boolean isClosed() {
        return closed;
    }
    
    @Override
    public void close(SessionExecutor executor) {
        try {
            executor.noop().get();
            synchronized (this) {
                if (!closed) {
                    closeImmediately(executor);
                    closed = true;
                }
            }
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }
}
