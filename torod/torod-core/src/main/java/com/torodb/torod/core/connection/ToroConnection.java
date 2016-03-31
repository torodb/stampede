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

import com.google.common.collect.FluentIterable;
import com.torodb.torod.core.Session;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import java.io.Closeable;
import java.util.Collection;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.json.JsonObject;

/**
 *
 */
@NotThreadSafe
public interface ToroConnection extends Closeable {
    
    /**
     * Close the connection and rollback its changes since last commit.
     */
    @Override
    public void close();
    
    @Nonnull
    public ToroTransaction createTransaction(TransactionMetainfo metainfo) throws ImplementationDbException;

    @Nonnull
    public UserCursor getCursor(CursorId cursorId) throws CursorNotFoundException;
    
    /**
     * Opens a unlimited cursor that iterates over the given query.
     * <p>
     * @param collection
     * @param queryCriteria if null, all documents are returned
     * @param projection    if null, all fields are returned
     * @param numberToSkip
     * @param autoclose
     * @param hasTimeout
     * @return
     * @see SessionTransaction#readAll(String, QueryCriteria)
     * @throws ToroException
     */
    @Nonnull
    public UserCursor openUnlimitedCursor(
            @Nonnull String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int numberToSkip,
            boolean autoclose,
            boolean hasTimeout
    ) throws ToroException;

    /**
     * Opens a limited cursor that iterates over the given query.
     * <p>
     * @param collection
     * @param queryCriteria if null, all documents are returned
     * @param projection    if null, all fields are returned
     * @param numberToSkip
     * @param limit         must be higher than 0
     * @param autoclose
     * @param hasTimeout
     * @return
     * @see SessionTransaction#readAll(String, QueryCriteria)
     * @throws ToroException
     */
    @Nonnull
    public UserCursor openLimitedCursor(
            @Nonnull String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int numberToSkip,
            int limit,
            boolean autoclose,
            boolean hasTimeout
    ) throws ToroException;
    
    /**
     * Creates an iterator that iterates over all collection metainformation on
     * the database.
     * @return 
     */
    @Nonnull
    public FluentIterable<CollectionMetainfo> getCollectionsMetainfoCursor() throws ToroException;
    
    @Nonnull
    public Session getSession();

    /**
     * Checks whether a collection already exists. If it doesn't, it creates the collection
     * @param collection The name of the collection
     * @param otherInfo A JSON document with extra information
     * @return true if the collection was created, false if it already existed
     */
    public boolean createCollection(@Nonnull String collection, @Nullable JsonObject otherInfo);
    
    @Nonnull
    public Collection<String> getCollections();
    
    public boolean dropCollection(
            @Nonnull String collection
    );
    
}
