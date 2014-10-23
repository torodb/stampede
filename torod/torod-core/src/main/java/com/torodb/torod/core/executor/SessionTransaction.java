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

import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.SplitDocument;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public interface SessionTransaction extends Closeable {

    Future<?> rollback();
    
    Future<?> commit();
    
    @Override
    void close();
    
    boolean isClosed();
    
    /**
     * Inserts the given documents in the database.
     * 
     * Documents must belong to the given collection.
     * 
     * @param collection
     * @param splitDoc
     * @param mode
     * @return
     * @throws ToroTaskExecutionException 
     */
    Future<InsertResponse> insertSplitDocuments(
            String collection,
            Collection<SplitDocument> splitDoc,
            WriteFailMode mode)
            throws ToroTaskExecutionException;

    Future<Void> query(
            @Nonnull String collection,
            @Nonnull CursorId cursorId,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection
    );

    /**
     *
     * @param cursorId
     * @param limit
     * @return a list of split documents retrieved from the cursor. Almost the given limit of documents are returned.
     *         less than the given limit of documents are returned iff the cursor reach its end.
     * @throws ToroTaskExecutionException
     */
    Future<List<? extends SplitDocument>> readCursor(
            CursorId cursorId,
            int limit
    ) throws ToroTaskExecutionException;

    /**
     * Completly read the given cursor.
     * <p>
     * @param cursorId
     * @return a list of split documents retrieved from the cursor.
     * @throws ToroTaskExecutionException
     */
    Future<List<? extends SplitDocument>> readAllCursor(
            CursorId cursorId
    ) throws ToroTaskExecutionException;

    public Future<DeleteResponse> delete(
            @Nonnull String collection, 
            @Nonnull List<? extends DeleteOperation> deletes, 
            @Nonnull WriteFailMode mode
    );

    Future<?> closeCursor(
            CursorId cursorId
    ) throws ToroTaskExecutionException;

    Future<Integer> countRemainingDocs(CursorId cursorId);
}
