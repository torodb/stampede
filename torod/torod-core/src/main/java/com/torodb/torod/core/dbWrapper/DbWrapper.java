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
package com.torodb.torod.core.dbWrapper;

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public interface DbWrapper {

    /**
     * Initializes this object.
     * <p>
     * This method is called when ToroD starts to retrieve some internal
     * information from the database and setup inner data structures.
     * @throws ImplementationDbException upon detecting an incompatible version of the database.
     */
    public void initialize() throws ImplementationDbException;

    /**
     * Returns a {@link DbConnection}.
     * <p>
     * Caller owns the connection and he must call {@link DbConnection#close()}
     * to release it.
     * <p>
     * The calling thread will be blocked until a SQL connection is usable if
     * the connection pooler is blocker.
     * <p>
     * @throws ImplementationDbException
     * @return
     */
    @Nonnull
    public DbConnection consumeSessionDbConnection() throws
            ImplementationDbException;

    /**
     * Retruns the {@link DbConnection} that is reserved to execute system
     * actions.
     * <p>
     * Only a system thread should use this connection.
     * <p>
     * @throws ImplementationDbException
     * @return
     */
    public DbConnection getSystemDbConnection() throws ImplementationDbException;

    /**
     * Executes a openGlobalCursor and return a cursor to the result.
     * <p>
     * This cursor must be closed onces it is not needed.
     * <p>
     * @param collection
     * @param cursorId
     * @param filter
     * @param projection
     * @param maxResults a non negative integer, the upper size of elements to
     *                   return or 0 to return all documents.
     * @return A cursor to the openGlobalCursor result. This cursor must be closed once it
         is not needed.
     * @throws ImplementationDbException
     * @throws UserDbException
     */
    @Nonnull
    public Cursor openGlobalCursor(
            @Nonnull String collection,
            @Nonnull CursorId cursorId,
            @Nullable QueryCriteria filter,
            @Nullable Projection projection,
            @Nonnegative int maxResults
    ) throws ImplementationDbException, UserDbException;

    /**
     * Returns the {@link Cursor cursor} represented by the given
     * {@linkplain CursorId cursor id}
     * <p>
     * @param cursorId
     * @return
     * @throws IllegalArgumentException  If there is no cursor associated with
     *                                   the given cursor id
     * @throws ImplementationDbException
     */
    @Nonnull
    public Cursor getGlobalCursor(CursorId cursorId) throws IllegalArgumentException;

}
