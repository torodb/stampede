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

package com.torodb.torod.core.dbMetaInf;

import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.subdocument.SubDocType;
import java.util.concurrent.Future;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.json.JsonValue.ValueType;

/**
 *
 */
@ThreadSafe
public interface DbMetaInformationCache {

    /**
     * This method must be called to guarantee the database is prepared to work with elements of type {@link ValueType}.
     * <p>
     * This method is idempotent.
     * <p>
     * @param sessionExecutor
     * @param type            the type that will be used
     * @param collection      collection to which the type belongs
     */
    public void createSubDocTypeTable(
            @Nonnull SessionExecutor sessionExecutor,
            @Nonnull String collection,
            @Nonnull SubDocType type
    );

    /**
     * Reserves a given number of ids that can be used as document id.
     * <p>
     * The returned value is the first reserved id. If the given first id is <em>firstId</em>, all ids in the rank
     * <code>[firstId..firstId + neededIds)</code> are granted to the invokator if this method.
     * <p>
     * @param sessionExecutor the executor of the session that is requesting the ids
     * @param collection      collection to which the documents belongs
     * @param neededIds       number of ids the client want to reserve.
     * @return the first reserved id
     * @throws IllegalArgumentException If neededIds is negative or if the collection is not created.
     */
    public int reserveDocIds(
            @Nonnull SessionExecutor sessionExecutor,
            @Nonnull String collection,
            @Nonnegative int neededIds
    ) throws IllegalArgumentException;

    /**
     * This method must be called to guarantee that the database is prepared to work with the given collection (in the
     * MongoDB sense).
     * <p>
     * This method is idempotent.
     * <p>
     * @param sessionExecutor
     * @param collection
     */
    public void createCollection(
            @Nonnull SessionExecutor sessionExecutor,
            @Nonnull String collection);

    public void initialize();
    
    public void shutdown();
    
    public void shutdownNow();
}
