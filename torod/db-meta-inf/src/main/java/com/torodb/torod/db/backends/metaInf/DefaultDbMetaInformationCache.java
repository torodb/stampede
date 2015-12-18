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

package com.torodb.torod.db.backends.metaInf;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.executor.ExecutorFactory;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.SubDocType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.json.JsonObject;

/**
 *
 */
@ThreadSafe
public class DefaultDbMetaInformationCache implements DbMetaInformationCache {

    static final int INITIAL_USED_ID = -1;

    private final ExecutorFactory executorFactory;

    private final ConcurrentMap<String, Object> createdCollections;
    private final ConcurrentMap<String, Long> creationCollectionPendingJobs;
    private final Map<String, CollectionMetaInfo> collectionMetaInfoMap;

    private final Lock collectionCreationLock;
    private final ReservedIdHeuristic reserveIdHeuristic;
    private final ReservedIdInfoFactory reservedIdInfoFactory;

    @Inject
    DefaultDbMetaInformationCache(
            ExecutorFactory executorFactory,
            ReservedIdHeuristic subDocTypeIdHeuristic,
            ReservedIdInfoFactory tableMetaInfoFactory
    ) {
        this.executorFactory = executorFactory;
        this.createdCollections = new MapMaker().makeMap();
        this.creationCollectionPendingJobs = new MapMaker().makeMap();
        this.collectionMetaInfoMap = Maps.newHashMap();

        this.collectionCreationLock = new ReentrantLock();
        this.reserveIdHeuristic = subDocTypeIdHeuristic;
        this.reservedIdInfoFactory = tableMetaInfoFactory;
    }

    @Override
    public void initialize() {
        try {
            Map<String, Integer> collectionInfo = executorFactory.getSystemExecutor().findCollections().get();

            for (Map.Entry<String, Integer> entry : collectionInfo.entrySet()) {
                String collection = entry.getKey();
                int lastUsedId = entry.getValue();

                startCollectionCreation(collection, lastUsedId);
                markCollectionAsCreated(collection);
            }
        } catch (InterruptedException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void shutdown() {
        Futures.immediateFuture(null);
    }

    @Override
    public void shutdownNow() {
    }

    @Override
    public boolean collectionExists(@Nonnull String collection) {
        return createdCollections.containsKey(collection);
    }

    private boolean collectionExists(@Nonnull SessionExecutor sessionExecutor, @Nonnull String collection) {
        if (collectionExists(collection)) {
            return true;
        }
        Long tick = creationCollectionPendingJobs.get(collection);
        if (tick != null) {
            sessionExecutor.pauseUntil(tick);
            return true;
        }

        return false;
    }

    @Override
    public Set<String> getCollections() {
        return Collections.unmodifiableSet(createdCollections.keySet());
    }

    @Override
    public boolean createCollection(
            @Nonnull SessionExecutor sessionExecutor, 
            @Nonnull String collectionName, 
            @Nullable JsonObject other) {
        if (collectionName == null || collectionName.isEmpty()) {
            throw new IllegalArgumentException("The collection must be non null and non empty");
        }

        if(collectionExists(sessionExecutor, collectionName)) {
            return false;
        }

        collectionCreationLock.lock();
        try {
            if(collectionExists(sessionExecutor, collectionName)) {
                return false;
            }

            startCollectionCreation(collectionName, INITIAL_USED_ID);

            Future<?> response = executorFactory.getSystemExecutor().createCollection(
                    collectionName,
                    other,
                    
                    new SystemExecutor.CreateCollectionCallback() {

                        @Override
                        public void createdCollection(String collection) {
                            markCollectionAsCreated(collection);
                            creationCollectionPendingJobs.remove(collection);
                        }
                    }
            );

            Long tick = executorFactory.getSystemExecutor().getTick();
            creationCollectionPendingJobs.put(collectionName, tick);

            if (response.isDone()) {
                response.get();
                creationCollectionPendingJobs.remove(collectionName);
            } else {
                sessionExecutor.pauseUntil(tick);
            }
        } catch (ToroTaskExecutionException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        catch (InterruptedException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } finally {
            collectionCreationLock.unlock();
        }

        return true;
    }

    @Override
    public boolean dropCollection(SessionExecutor sessionExecutor, String collection) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException("The collection must be non null and non empty");
        }
        
        if (!collectionExists(collection)) {
            return false;
        }
        
        collectionCreationLock.lock();
        try {
            if (!collectionExists(sessionExecutor, collection)) {
                return false;
            }
            Future<?> response = executorFactory.getSystemExecutor().dropCollection(collection);
            response.get();
            
            assert !creationCollectionPendingJobs.containsKey(collection);
            
            Object removed = createdCollections.remove(collection);
            assert removed != null;
            removed = collectionMetaInfoMap.remove(collection);
            assert removed != null;
            
            return true;
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        } finally {
            collectionCreationLock.unlock();
        }
    }

    @Override
    public void createSubDocTypeTable(@Nonnull SessionExecutor sessionExecutor, @Nonnull String collection, @Nonnull SubDocType type) {
        //Preconditions
        if (type == null) {
            throw new IllegalArgumentException("The subdocument type must be non null");
        }
        if (!creationCollectionPendingJobs.containsKey(collection) && !createdCollections.containsKey(collection)) {
            throw new IllegalArgumentException("Collection '" + collection + "' is not created or being created");
        }

        //Business logic
        CollectionMetaInfo collectionMetaInfo = collectionMetaInfoMap.get(collection);
        assert collectionMetaInfo != null;

        collectionMetaInfo.createSubDocTypeTable(sessionExecutor, type);
    }

    @Override
    public int reserveDocIds(@Nonnull SessionExecutor sessionExecutor, @Nonnull String collection, @Nonnegative int neededIds) throws IllegalArgumentException {
        CollectionMetaInfo collectionMetaInfo = collectionMetaInfoMap.get(collection);
        if (collectionMetaInfo == null) {
            throw new IllegalArgumentException("Collection "+ collection + " hasn't been created");
        }
        
        return collectionMetaInfo.reserveDocId(sessionExecutor, neededIds);
    }

    private void startCollectionCreation(String collection, int initialId) {
        ReservedIdInfo reservedIdInfo = reservedIdInfoFactory.createReservedIdInfo(initialId, initialId);

        Object old = collectionMetaInfoMap.put(
                collection,
                new CollectionMetaInfo(
                        collection,
                        reservedIdInfo,
                        executorFactory.getSystemExecutor(),
                        reserveIdHeuristic)
        );
        if (old != null) {
            //TODO: Study exceptions
            throw new IllegalStateException("There was a previous meta information map associated to the new collection '" + collection + "'");
        }
    }

    private void markCollectionAsCreated(String collection) {
        createdCollections.put(collection, Boolean.TRUE);
    }
}
