/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.toro.torod.cursors;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.MapMaker;
import com.torodb.torod.core.cursors.CursorProperties;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.config.TorodConfig;
import com.torodb.torod.core.cursors.InnerCursorManager;
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@ThreadSafe
public class DefaultInnerCursorManager implements InnerCursorManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultInnerCursorManager.class);
    private static final long FIRST_CURSOR_ID = 0;
    static final int OLD_CACHE_EVICTION_PERIOD = 10000;

    private final ConcurrentMap<CursorId, CursorProperties> withoutTimeout;
    private Cache<CursorId, CursorProperties> withTimeout;
    /**
     * A set that stores caches with old timeouts.
     * 
     * Each time {@linkplain #setTimeout(long) timeout changes}, the old cache is stored here and a new cache is created.
     * 
     * This set is thread safe and is evicted periodically.
     */
    private final Set<Cache<CursorId, CursorProperties>> oldCaches;
    private final AtomicInteger counterToOldCacheEviction = new AtomicInteger();
    private final MyRemovalListener removalListener;
    private final DbWrapper dbWrapper;
    private long actualTimeout;
    /**
     * Stores the number of read elements for each cursor (autocloseable or not).
     */
    private final ConcurrentMap<CursorId, AtomicInteger> readElementMap;
    private final AtomicLong cursorIdProvider;
    private final Ticker ticker;

    @Inject
    public DefaultInnerCursorManager(TorodConfig config, DbWrapper dbWrapper) {
        this.actualTimeout = config.getDefaultCursorTimeout();
        this.removalListener = new MyRemovalListener();
        this.ticker = Ticker.systemTicker();
        this.dbWrapper = dbWrapper;
        
        withoutTimeout = new MapMaker()
                .makeMap();
        withTimeout = createCache(actualTimeout);
        oldCaches = Collections.newSetFromMap(new MapMaker().<Cache<CursorId, CursorProperties>, Boolean>makeMap());

        readElementMap = new MapMaker().makeMap();

        cursorIdProvider = new AtomicLong(FIRST_CURSOR_ID);
    }

    /**
     * Created for test purpose.
     * @param config
     * @param sessionTransaction
     * @param ticker 
     */
    protected DefaultInnerCursorManager(
            TorodConfig config, 
            DbWrapper dbWrapper, 
            Ticker ticker) {
        this.actualTimeout = config.getDefaultCursorTimeout();
        this.removalListener = new MyRemovalListener();
        this.ticker = ticker;
        this.dbWrapper = dbWrapper;
        
        withoutTimeout = new MapMaker()
                .makeMap();
        withTimeout = createCache(actualTimeout);
        oldCaches = Collections.newSetFromMap(new MapMaker().<Cache<CursorId, CursorProperties>, Boolean>makeMap());

        readElementMap = new MapMaker().makeMap();

        cursorIdProvider = new AtomicLong(FIRST_CURSOR_ID);
    }

    @Override
    public CursorProperties openUnlimitedCursor(boolean hasTimeout, boolean autoclose) {
        CursorProperties cursorProps = new CursorProperties(
                new CursorId(cursorIdProvider.incrementAndGet()),
                hasTimeout,
                autoclose
        );

        storeCursor(cursorProps);

        return cursorProps;
    }

    @Override
    public CursorProperties openLimitedCursor(boolean hasTimeout, boolean autoclose, int limit) {
        CursorProperties cursorProps = new CursorProperties(
                new CursorId(cursorIdProvider.incrementAndGet()),
                hasTimeout,
                limit,
                autoclose
        );

        storeCursor(cursorProps);

        return cursorProps;
    }

    private void storeCursor(CursorProperties cursor) throws IllegalArgumentException {
        eventAccess();
        if (!cursor.hasTimeout()) {
            withoutTimeout.put(cursor.getId(), cursor);
        } else {
            withTimeout.put(cursor.getId(), cursor);
        }
        readElementMap.put(cursor.getId(), new AtomicInteger(0));
    }
    
    @Override
    public boolean exists(CursorId cursorId) {
        eventAccess();
        /*
        * It is not very efficient to look for the cursor in this way, but it is better to do it in this way to
        * be sure that pendient clean up operations are called in caches, even a cursor without timeout is accesed.
        */
        boolean result = false;
        for (Cache<CursorId, CursorProperties> cache : oldCaches) {
            if (cache.getIfPresent(cursorId) != null) {
                result = true;
            }
        }
        if (withTimeout.getIfPresent(cursorId) != null) {
            result = true;
        }
        if (withoutTimeout.containsKey(cursorId)) {
            result = true;
        }
        return result;
    }

    @Override
    public void close(CursorId cursorId) throws IllegalArgumentException {
        eventAccess();
        CursorProperties removed = withoutTimeout.remove(cursorId);
        if (removed != null) {
            removalListener.onRemoval(cursorId, removed);
        }
        withTimeout.invalidate(cursorId);
        for (Cache<CursorId, CursorProperties> cache : oldCaches) {
            cache.invalidate(cursorId);
        }
    }

    @Override
    public boolean notifyRead(CursorId cursorId, int readSize) throws IllegalArgumentException {
        if (readSize < 0) {
            throw new IllegalArgumentException("The number of read results must be non negative");
        }
        CursorProperties cursor = getCursor(cursorId);
        
        int newReadElements = readElementMap.get(cursorId).addAndGet(readSize);
        if (cursor.isAutoclose() && cursor.hasLimit() && newReadElements >= cursor.getLimit()) {
            close(cursorId);

            if (newReadElements > cursor.getLimit()) {
                LOGGER.warn("It was expected " + cursor.getLimit() + " as "
                        + "limit of the cursor, but " + newReadElements 
                        + " has been read");
            }

            return true;
        }
        return false;
    }

    @Override
    public int getReadElements(CursorId cursorId) throws IllegalArgumentException {
        CursorProperties cursor = getCursor(cursorId);
        assert cursor != null;
        
        AtomicInteger readElements = readElementMap.get(cursorId);
        if (readElements == null) {
            throw new AssertionError(cursorId + " is open, but it is not contained in the read elements map!");
        }
        return readElements.get();
    }

    @Override
    public boolean notifyAllRead(CursorId cursorId) throws IllegalArgumentException {
        CursorProperties cursor = getCursor(cursorId);
        if (!cursor.isAutoclose()) {
            return false;
        }

        close(cursorId);
        return true;
    }

    @Override
    public CursorProperties getCursor(CursorId cursorId) throws IllegalArgumentException {
        eventAccess();
        /*
        * It is not very efficient to look for the cursor in this way, but it is better to do it in this way to
        * be sure that pendient clean up operations are called in caches, even a cursor without timeout is accesed.
        */
        CursorProperties result = null;
        CursorProperties temp;
        for (Cache<CursorId, CursorProperties> cache : oldCaches) {
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
            throw new IllegalArgumentException("There is no stored cursor with id '" + cursorId + "'");
        }
        
        return result;
    }

    @Override
    public void setTimeout(long millis) {
        if (millis == actualTimeout) {
            return;
        }
        oldCaches.add(withTimeout);

        this.actualTimeout = millis;
        withTimeout = createCache(millis);
    }

    @Override
    public void cleanUp() {
        withTimeout.cleanUp();
        for (Cache<CursorId, CursorProperties> cache : oldCaches) {
            cache.cleanUp();
        }
        removeUnusedCaches();
    }

    @Override
    public void close() {
        for (Cache<CursorId, CursorProperties> cache : oldCaches) {
            cache.invalidateAll();
        }
        withTimeout.invalidateAll();
        for (Map.Entry<CursorId, CursorProperties> entry : withoutTimeout.entrySet()) {
            removalListener.onRemoval(entry.getKey(), entry.getValue());
        }
        withoutTimeout.clear();
    }
    
    /**
     * Returns the size of oll caches set.
     * 
     * This function exists for testing purpose.
     * 
     * @return 
     */
    int getOldCachesSize() {
        return oldCaches.size();
    }
    
    private void eventAccess() {
        int counter = counterToOldCacheEviction.incrementAndGet();
        if (counter % OLD_CACHE_EVICTION_PERIOD == 0) {
            removeUnusedCaches();
        }
    }
    
    private void removeUnusedCaches() {
        Iterator<Cache<CursorId, CursorProperties>> caches = oldCaches.iterator();
        
        while (caches.hasNext()) {
            Cache<CursorId, CursorProperties> cache = caches.next();
            if (cache.size() == 0) {
                caches.remove();
            }
        }
        counterToOldCacheEviction.set(0);
    }

    private Cache<CursorId, CursorProperties> createCache(long timeout) {
        return CacheBuilder.newBuilder()
                .expireAfterAccess(timeout, TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .ticker(ticker)
                .build();
    }

    private class MyRemovalListener implements RemovalListener<CursorId, CursorProperties> {

        @Override
        public void onRemoval(RemovalNotification<CursorId, CursorProperties> notification) {
            onRemoval(notification.getKey(), notification.getValue());
        }

        public void onRemoval(CursorId key, CursorProperties value) {
            try {
                readElementMap.remove(key);
                Cursor globalCursor = dbWrapper.getGlobalCursor(key);
                globalCursor.close();
            } catch (ImplementationDbException ex) {
                //TODO: Change exceptions
                throw new RuntimeException(ex); //this exception will be logged and swallowed by the cache!!!!
            } catch (IllegalArgumentException ex) {
                LOGGER.warn("Cursor "+key+" has been closed before");
            }

        }

    }

}
