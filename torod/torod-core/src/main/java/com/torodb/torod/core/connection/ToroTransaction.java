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

package com.torodb.torod.core.connection;

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Future;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public interface ToroTransaction extends Closeable {

    public Future<?> rollback();

    public Future<?> commit();

    @Override
    public void close();

    /**
     * Insert the given documents in the collection.
     * <p>
     * @param collection
     * @param documents
     * @param mode
     * @return
     */
    public Future<InsertResponse> insertDocuments(
            @Nonnull String collection,
            @Nonnull Iterable<ToroDocument> documents,
            WriteFailMode mode
    );

    /**
     * Makes a query without limit.
     * <p>
     * @param collection
     * @param queryCriteria if null, all documents are returned
     * @param projection    if null, all fields are returned
     * @param numberToSkip
     * @param autoclose
     * @param hasTimeout
     * @return
     */
    public CursorId query(
            @Nonnull String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int numberToSkip,
            boolean autoclose,
            boolean hasTimeout
    );

    /**
     * Makes a query with limit.
     * <p>
     * @param collection
     * @param queryCriteria if null, all documents are returned
     * @param projection    if null, all fields are returned
     * @param numberToSkip
     * @param limit         must be higher than 0
     * @param autoclose
     * @param hasTimeout
     * @return
     */
    public CursorId query(
            @Nonnull String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int numberToSkip,
            int limit,
            boolean autoclose,
            boolean hasTimeout
    );

    /**
     *
     * @param cursorId
     * @param limit    must be a positive integer (>= 1)
     * @return
     */
    public List<ToroDocument> readCursor(
            @Nonnull CursorId cursorId,
            @Nonnegative int limit
    );

    public List<ToroDocument> readAllCursor(
            @Nonnull CursorId cursorId
    );

    public void closeCursor(CursorId cursorId);
    
    public Future<Integer> getPosition(CursorId cursorId);
    
    public Future<Integer> countRemainingDocs(CursorId cursorId);

    /**
     * Deletes documents that fulfil the given condition.
     * <p>
     * @param collection
     * @param deletes
     * @param mode
     * @return A {@linkplain Future future} that can be used to wait until the
     *         action is commited.
     */
    public Future<DeleteResponse> delete(
            @Nonnull String collection,
            @Nonnull List<? extends DeleteOperation> deletes,
            @Nonnull WriteFailMode mode
    );

    public Future<UpdateResponse> update(
            @Nonnull String collection,
            @Nonnull List<? extends UpdateOperation> updates,
            @Nonnull WriteFailMode mode
    );
    
    /**
    * Creates an empty collection
    * <p>
    * @param collection
    * @return
    */
    public void createEmptyCollection(@Nonnull String collection);

}
