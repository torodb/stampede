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

package com.torodb.torod.core.executor;

import com.torodb.torod.core.subdocument.SubDocType;
import java.util.Map;
import java.util.concurrent.Future;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.json.JsonObject;

/**
 *
 */
public interface SystemExecutor {

    Future<?> createCollection(
            String collection,
            @Nullable JsonObject other,
            @Nullable CreateCollectionCallback callback)
            throws ToroTaskExecutionException;
    
    public Future<?> dropCollection(
            @Nonnull String collection
    );

    Future<?> createSubDocTable(
            String collection,
            SubDocType type,
            @Nullable CreateSubDocTypeTableCallback callback)
            throws ToroTaskExecutionException;

    Future<?> reserveDocIds(
            String collection,
            @Nonnegative int idsToReserve,
            @Nullable ReserveDocIdsCallback callback)
            throws ToroTaskExecutionException;

    Future<Map<String, Integer>> findCollections();

    /**
     * Returns a number that identifies the last pending job.
     * <p>
     * Subsequent calls to this method will return the same or a higher value, but never a lower one.
     * <p>
     * This number can be used as input to {@link UserExecutor#pauseUntil(int) }
     * <p>
     * @return
     * @see UserExecutor#pauseUntil(int)
     */
    long getTick();
    
    public static interface CreateCollectionCallback {

        public void createdCollection(String collection);
    }

    public static interface CreateSubDocTypeTableCallback {

        public void createSubDocTypeTable(String collection, SubDocType type);
    }

    public static interface ReserveDocIdsCallback {

        public void reservedDocIds(String collection, @Nonnegative int idsToReserve);
    }
}
