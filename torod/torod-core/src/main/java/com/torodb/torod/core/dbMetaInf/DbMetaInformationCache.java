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
import java.util.Set;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.json.JsonObject;
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
     * Checks whether a collection already exists in the database. There's no guarantee that a non existing collection
     * may exist at a later time, but if this method returns true, the collection is already created
     * @param collection
     * @return True if the collection exists, false if (currently) it doesn't
     */
    public boolean collectionExists(@Nonnull String collection);

    /**
     * This method must be called to guarantee that the database is prepared to work with the given collection (in the
     * MongoDB sense).
     * <p>
     * This method is idempotent.
     * <p>
     * @param sessionExecutor
     * @param collectionName
     * @param other
     * @return Returns whether the collection was created (returns false if the collection already existed)
     */
    public boolean createCollection(
            @Nonnull SessionExecutor sessionExecutor,
            @Nonnull String collectionName,
            @Nullable JsonObject other
    );

    public boolean dropCollection(
            @Nonnull SessionExecutor sessionExecutor, 
            @Nonnull String collection);
    
    @Nonnull
    public Set<String> getCollections();
    
    public void initialize();
    
    public void shutdown();
    
    public void shutdownNow();
}
