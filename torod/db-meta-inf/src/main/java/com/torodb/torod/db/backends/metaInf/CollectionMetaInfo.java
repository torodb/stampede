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
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.executor.SystemExecutor.CreateSubDocTypeTableCallback;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.subdocument.SubDocType;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class CollectionMetaInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionMetaInfo.class);

    private final ReservedIdInfo info;
    private final ConcurrentMap<SubDocType, Long> creationPendingJobs;
    private final Set<SubDocType> createdSubDocTypes;
    private final String collection;
    private final SystemExecutor systemExecutor;
    private final ReservedIdHeuristic heuristic;
    private final Lock lock;
    private final CreateSubDocTypeTableCallback CREATE_SUB_DOC_TYPE_CALLBACK = new CreateSubDocTypeTableCallbackImpl();

    public CollectionMetaInfo(String collection, ReservedIdInfo info, SystemExecutor systemExecutor, ReservedIdHeuristic heuristic) {
        this.info = info;
        this.collection = collection;
        this.systemExecutor = systemExecutor;
        this.heuristic = heuristic;

        creationPendingJobs = new MapMaker().makeMap();
        createdSubDocTypes = Collections.newSetFromMap(new MapMaker().<SubDocType, Boolean>makeMap());

        this.lock = new ReentrantLock();
    }

    void createSubDocTypeTable(@Nonnull SessionExecutor sessionExecutor, @Nonnull SubDocType type) {
        if (createdSubDocTypes.contains(type)) { //it was previously created
            return; //so nothing has to be done
        }

        Long tick = creationPendingJobs.get(type);
        if (tick != null) { //if we are creating the table right now...
            sessionExecutor.pauseUntil(tick); //wait until the table is created
            return;
        }
        LOGGER.debug("{}.{} table was not created", collection, type);

        lock.lock();
        try {
            if (createdSubDocTypes.contains(type)) { //another thread oredered the creation and it has already been executed
                LOGGER.debug("{}.{} table was created while I was waiting", collection, type);
                return; //so nothing has to be done
            }
            tick = creationPendingJobs.get(type);
            if (tick != null) { //another thread ordered the table creation
                LOGGER.debug("{}.{} table creation has been scheduled while I was waiting", collection, type);
                sessionExecutor.pauseUntil(tick); //so this thread must wait until the creation is executed
                return;
            }
            //this thread has the lock and nobody ordered the creation before it get the lock, so this thread must order the creation
            LOGGER.debug("I will schedule creation of {}.{} table", collection, type);

            Future<?> future = systemExecutor.createSubDocTable(collection,
                    type,
                    CREATE_SUB_DOC_TYPE_CALLBACK
            );
            LOGGER.debug("{}.{} table creation has been scheduled", collection, type);
            Futures.getUnchecked(future);
            LOGGER.debug("{}.{} table creation has been executed", collection, type);
            createdSubDocTypes.add(type);

        } catch (ToroTaskExecutionException ex) {
            //TODO: Change exception
            throw new RuntimeException(ex);
        } finally {
            lock.unlock();
        }
    }

    int reserveDocId(SessionExecutor sessionExecutor, int neededIds) {
        int firstFreeId = info.getAndAddLastUsedId(neededIds) + 1;

        Future<?> blocker = reserveMoreIdsIfNeeded(collection, info);
        if (blocker == null) {
            LOGGER.trace("I have consumed {} doc ids of collection {}. I didn't need to wait for new ids", neededIds, collection);
        } else {
            Long tick = systemExecutor.getTick();
            sessionExecutor.pauseUntil(tick);
            LOGGER.debug("I have consumed {} doc ids of collection {}. I needed to wait for new ids", neededIds, collection);
        }

        return firstFreeId;
    }

    @Nullable
    private Future<?> reserveMoreIdsIfNeeded(String collection, ReservedIdInfo metaInf) {
        assert metaInf != null;
        int lastCachedId = metaInf.getLastCachedId();
        int lastUsedId = metaInf.getLastUsedId();
        boolean blocker = lastCachedId < lastUsedId;
        int idsToReserve = heuristic.evaluate(lastUsedId, lastCachedId);
        Future<?> result = null;

        if (idsToReserve > 0) {
            LOGGER.debug("Heuristic said {} new ids are needed. Difference between lastCachedId and lastUsedId is {}", idsToReserve, lastCachedId - lastUsedId);
            try {
                result = systemExecutor.reserveDocIds(
                        collection,
                        idsToReserve,
                        new SystemExecutor.ReserveDocIdsCallback() {

                            @Override
                            public void reservedDocIds(String collection, int idsToReserve) {
                                incrementLastReservedId(collection, idsToReserve);
                            }
                        });
            } catch (ToroTaskExecutionException ex) {
                //TODO: Change exception
                throw new RuntimeException(ex);
            }
        }
        if (idsToReserve < 0) {
            LOGGER.warn("Heuristic said {} new ids are needed. This number shouldn't be negative!", idsToReserve);
        }

        if (blocker) {
            int newLastCached = lastCachedId + idsToReserve;
            if (newLastCached < lastUsedId) {
                throw new AssertionError(getClass() + "#evaluateReserveHeuristic said " + idsToReserve + " ids must be "
                        + "cached, but " + idsToReserve + " + lastCachedId (=" + lastCachedId + ") is lower than the "
                        + "first free id (=" + lastUsedId + ")!");
            }
            if (idsToReserve == 0) {
                assert result == null;
                throw new AssertionError(getClass() + "#evaluateReserveHeuristic said no new ids are needed but more "
                        + "ids are needed!");
            } else {
                assert result != null;
                return result;
            }
        }
        return null;
    }

    
    private void incrementLastReservedId(String collection, @Nonnegative int increment) {
        //Preconditions
        if (increment < 0) {
            throw new IllegalArgumentException("The increment must not be negative");
        }
        if (info.getLastCachedId() + increment < 0) {
            throw new IllegalArgumentException("Last cached id would overflow with this increment");
        }

        info.getAndAddLastCachedId(increment);
    }

    private static class CreateSubDocTypeTableCallbackImpl implements CreateSubDocTypeTableCallback {

        public CreateSubDocTypeTableCallbackImpl() {
        }

        @Override
        public void createSubDocTypeTable(String collection, SubDocType type) {
        }
    }
}
