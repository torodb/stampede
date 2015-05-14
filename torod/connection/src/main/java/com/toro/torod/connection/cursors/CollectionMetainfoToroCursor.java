
package com.toro.torod.connection.cursors;

import com.google.common.collect.Lists;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class CollectionMetainfoToroCursor extends DefaultToroCursor<CollectionMetainfo> {

    private final Future<List<CollectionMetainfo>> collectionMetainfoCursor;
    private List<CollectionMetainfo> _pendingElements;
    private int position;
    private final AtomicBoolean initialized;
    private volatile int maxElements;
    
    public CollectionMetainfoToroCursor(
            CursorId id, 
            boolean hasTimeout, 
            int limit, 
            boolean autoclosable,
            Future<List<CollectionMetainfo>> collectionMetainfoCursor) {
        super(id, hasTimeout, limit, autoclosable);
        this.collectionMetainfoCursor = collectionMetainfoCursor;
        this.initialized = new AtomicBoolean(false);
    }
    
    public CollectionMetainfoToroCursor(
            CursorId id, 
            boolean hasTimeout, 
            boolean autoclosable,
            Future<List<CollectionMetainfo>> collectionMetainfoCursor) {
        super(id, hasTimeout, autoclosable);
        this.collectionMetainfoCursor = collectionMetainfoCursor;
        this.initialized = new AtomicBoolean(false);
    }

    @Override
    public Class<? extends CollectionMetainfo> getType() {
        return CollectionMetainfo.class;
    }
    
    public synchronized List<CollectionMetainfo> getPendingElements() 
            throws InterruptedException, ExecutionException, ClosedToroCursorException {
        if (isClosed()) {
            throw new ClosedToroCursorException();
        }
        if (!initialized.get()) {
            _pendingElements = collectionMetainfoCursor.get();
            position = 0;
            maxElements = _pendingElements.size();
            initialized.set(true);
        }
        return _pendingElements;
    }

    @Override
    public List<CollectionMetainfo> readAll(SessionExecutor executor) throws ClosedToroCursorException {
        try {
            executor.noop().get();
            synchronized (this) {
                List<CollectionMetainfo> result = Lists.newArrayList(getPendingElements());
                
                _pendingElements = Collections.emptyList();
                position += result.size();
                
                if (position == getMaxElements() && isAutoclosable()) {
                    close(executor);
                }
                
                return result;
            }
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public List<CollectionMetainfo> read(SessionExecutor executor, int limit) throws ClosedToroCursorException {
        try {
            executor.noop().get();
            synchronized (this) {
                List<CollectionMetainfo> pendingElements = getPendingElements();
                limit = Math.min(pendingElements.size(), limit);
                List<CollectionMetainfo> result = Lists.newArrayList(pendingElements.subList(0, limit));
                
                _pendingElements = _pendingElements.subList(limit, _pendingElements.size());
                position += limit;
                
                if (isAutoclosable()) {
                    close(executor);
                }
                
                return result;
            }
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    protected synchronized void closeImmediately(SessionExecutor executor) {
        _pendingElements = Collections.emptyList();
    }

    @Override
    public int getPosition(SessionExecutor executor) {
        try {
            executor.noop().get();
            synchronized (this) {
                return position;
            }
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public int getMaxElements() {
        if (!initialized.get()) {
            try {
                getPendingElements();
            }
            catch (InterruptedException ex) {
                throw new ToroRuntimeException(ex);
            }
            catch (ExecutionException ex) {
                throw new ToroRuntimeException(ex);
            }
            catch (ClosedToroCursorException ex) {
                throw new ToroRuntimeException(ex);
            }
        }
        return maxElements;
    }
}
