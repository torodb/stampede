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

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public interface CursorManager {

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
     */
    public CursorId openUnlimitedCursor(
            @Nonnull String collection,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int numberToSkip,
            boolean autoclose,
            boolean hasTimeout
    );

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
     */
    public CursorId openLimitedCursor(
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

}
