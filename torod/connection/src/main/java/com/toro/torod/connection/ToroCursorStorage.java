
package com.toro.torod.connection;

import com.google.common.base.Ticker;
import com.google.common.cache.*;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.ToroCursor;
import com.torodb.torod.core.exceptions.*;
import com.torodb.torod.core.executor.SessionExecutor;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@ThreadSafe
public class ToroCursorStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToroCursorStorage.class);
    static final int OLD_CACHE_EVICTION_PERIOD = 10000;

    private final ConcurrentMap<CursorId, StoredToroCursorDelegator> withoutTimeout;
    private Cache<CursorId, StoredToroCursorDelegator> withTimeout;
    /**
     * A set that stores caches with old timeouts.
     * 
     * Each time {@linkplain #setTimeout(long) timeout changes}, the old cache
     * is stored here and a new cache is created.
     * 
     * This set is thread safe and is evicted periodically.
     */
    private final Set<Cache<CursorId, StoredToroCursorDelegator>> oldCaches;
    private final AtomicInteger counterToOldCacheEviction = new AtomicInteger();
    private final ToroCursorStorage.MyRemovalListener removalListener;
    private long actualTimeout;
    /**
     * Stores the number of read elements for each cursor (autocloseable or not).
     */
    private final ConcurrentMap<CursorId, AtomicInteger> readElementMap;
    private final Ticker ticker;
    private final ConcurrentLinkedQueue<StoredToroCursorDelegator> expiredCursors;

    public ToroCursorStorage(DbBackend config) {
        this.actualTimeout = config.getDefaultCursorTimeout();
        this.removalListener = new ToroCursorStorage.MyRemovalListener();
        this.ticker = Ticker.systemTicker();
        
        withoutTimeout = new MapMaker().makeMap();
        withTimeout = createCache(actualTimeout);
        oldCaches = Collections.newSetFromMap(
                new MapMaker().<Cache<CursorId, StoredToroCursorDelegator>, Boolean>makeMap()
        );

        readElementMap = new MapMaker().makeMap();
        this.expiredCursors = new ConcurrentLinkedQueue<StoredToroCursorDelegator>();
    }

    public void setTimeout(long millis) {
        if (millis == actualTimeout) {
            return;
        }
        oldCaches.add(withTimeout);

        this.actualTimeout = millis;
        withTimeout = createCache(millis);
    }
    
    public ToroCursor getCursor(CursorId cursorId) throws CursorNotFoundException {
        /*
         * It is not very efficient to look for the cursor in this way, but it
         * is better to do it in this way to be sure that pendient clean up
         * operations are called in caches, even a cursor without timeout is
         * accesed.
         */
        ToroCursor result = null;
        ToroCursor temp;
        for (Cache<CursorId, StoredToroCursorDelegator> cache : oldCaches) {
            temp = cache.getIfPresent(cursorId);
            if (result == null && temp != null) {
                result = temp;
            }
        }
        
        temp = withTimeout.getIfPresent(cursorId);
        if (result == null && temp != null) {
            result = temp;
        }
        
        if (result == null) {
            result = withoutTimeout.get(cursorId);
        }
        
        if (result == null) {
            throw new CursorNotFoundException(cursorId, "There is no stored cursor with id '" + cursorId + "'");
        }
        
        return result;
    }
    
    /**
     * Stores and returns a delegator of the given cursor.
     * 
     * The cursor returned is the one that has to be used, in other case,
     * memory leaks or unexpected close cursors can happen.
     * @param <E>
     * @param cursor
     * @param executor
     * @return 
     */
    public <E> ToroCursor<E> storeCursor(ToroCursor<E> cursor, SessionExecutor executor) {
        eventAccess(executor);
        
        StoredToroCursorDelegator delegator = new StoredToroCursorDelegator(cursor);
        
        if (!cursor.hasTimeout()) {
            withoutTimeout.put(cursor.getId(), delegator);
        } else {
            withTimeout.put(cursor.getId(), delegator);
        }
        readElementMap.put(cursor.getId(), new AtomicInteger(0));
        
        return delegator;
    }
    
    public void notifyUse(CursorId cursorId, SessionExecutor executor) throws CursorNotFoundException {
        getCursor(cursorId, executor);
    }

    public void notifyClose(CursorId id, SessionExecutor executor) {
        eventAccess(executor);
        withoutTimeout.remove(id);
        withTimeout.invalidate(id);
        for (Cache<CursorId, StoredToroCursorDelegator> cache : oldCaches) {
            cache.invalidate(id);
        }
    }
    
    /**
     * Close this object and returns all cursors that are pending to be closed.
     * @return 
     */
    public List<ToroCursor> close() {
        for (Cache<CursorId, StoredToroCursorDelegator> cache : oldCaches) {
            cache.invalidateAll();
        }
        withTimeout.invalidateAll();
        withoutTimeout.clear();
        
        List<ToroCursor> result = Lists.newArrayListWithCapacity(expiredCursors.size());
        for (StoredToroCursorDelegator expiredCursor : expiredCursors) {
            result.add(expiredCursor.delegate);
        }
        return result;
    }
    
    private Cache<CursorId, StoredToroCursorDelegator> createCache(long timeout) {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(timeout, TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .ticker(ticker)
                .build();
    }
    
    private void eventAccess(SessionExecutor executor) {
        int counter = counterToOldCacheEviction.incrementAndGet();
        if (counter % OLD_CACHE_EVICTION_PERIOD == 0) {
            removeUnusedCaches();
        }
        for (StoredToroCursorDelegator expiredCursor : expiredCursors) {
            expiredCursor.privateClose(executor);
        }
    }
    
    private void removeUnusedCaches() {
        Iterator<Cache<CursorId, StoredToroCursorDelegator>> caches = oldCaches.iterator();
        
        while (caches.hasNext()) {
            Cache<CursorId, StoredToroCursorDelegator> cache = caches.next();
            if (cache.size() == 0) {
                caches.remove();
            }
        }
        counterToOldCacheEviction.set(0);
    }
    
    private ToroCursor getCursor(CursorId cursorId, SessionExecutor executor) 
            throws CursorNotFoundException {
        eventAccess(executor);
        
        return getCursor(cursorId);
    }
    
    private void markAsExpired(StoredToroCursorDelegator cursor) {
        expiredCursors.add(cursor);
    }

    private class MyRemovalListener implements RemovalListener<CursorId, StoredToroCursorDelegator> {

        @Override
        public void onRemoval(RemovalNotification<CursorId, StoredToroCursorDelegator> notification) {
            switch (notification.getCause()) {
                case COLLECTED: {
                    LOGGER.warn("A value was garbage-collected before it was "
                            + "removed or closed");
                    break;
                }
                case SIZE: { //
                    LOGGER.warn("A cursor was removed because the maximum size "
                            + "of the cache has been reached, but the cursor"
                            + "cache should have no maximum size!");
                    break;
                }
                default: {
                    LOGGER.warn("A cursor was removed because an unknown reason!");
                    break;
                }
                case REPLACED: { //by contract, this should not happen
                    throw new AssertionError("A cursor cannot be replaced");
                }
                case EXPLICIT: { //by contact, if it was explicity removed, the caller shall call cursor.close too
                    break;
                }
                case EXPIRED: {
                    markAsExpired(notification.getValue());
                }
            }
        }
    }
    
    private class StoredToroCursorDelegator<E> implements ToroCursor<E> {
        private final ToroCursor<E> delegate;

        public StoredToroCursorDelegator(ToroCursor<E> delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<E> readAll(SessionExecutor executor) throws
                ClosedToroCursorException {
            try {
                notifyUse(getId(), executor);
            }
            catch (CursorNotFoundException ex) {
                throw new ToroImplementationException("Cursor is not stored!", ex);
            }
            return delegate.readAll(executor);
        }

        @Override
        public List<E> read(SessionExecutor executor, int limit) throws
                ClosedToroCursorException {
            try {
                notifyUse(getId(), executor);
            }
            catch (CursorNotFoundException ex) {
                throw new ToroImplementationException("Cursor is not stored!", ex);
            }
            return delegate.read(executor, limit);
        }

        @Override
        public void close(SessionExecutor executor) {
            notifyClose(getId(), executor);
            delegate.close(executor);
        }

        @Override
        public int getPosition(SessionExecutor executor) throws
                ClosedToroCursorException {
            try {
                notifyUse(getId(), executor);
            }
            catch (CursorNotFoundException ex) {
                throw new ToroImplementationException("Cursor is not stored!", ex);
            }
            return delegate.getPosition(executor);
        }

        @Override
        public int getMaxElements() throws UnknownMaxElementsException {
            return delegate.getMaxElements();
        }

        @Override
        public Class<? extends E> getType() {
            return delegate.getType();
        }

        @Override
        public CursorId getId() {
            return delegate.getId();
        }

        @Override
        public boolean hasTimeout() {
            return delegate.hasTimeout();
        }

        @Override
        public boolean hasLimit() {
            return delegate.hasLimit();
        }

        @Override
        public int getLimit() throws ToroImplementationException {
            return delegate.getLimit();
        }

        @Override
        public boolean isAutoclosable() {
            return delegate.isAutoclosable();
        }

        private void privateClose(SessionExecutor executor) {
            delegate.close(executor);
        }
        
    }

}
