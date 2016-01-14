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

import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.CollectionMetainfo;
import com.torodb.torod.core.subdocument.SplitDocument;
import java.io.Closeable;
import java.util.List;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 *
 */
@ThreadSafe
public interface SessionExecutor extends Closeable {
    
    /**
     * Close the executor and rollback its changes since last commit.
     */
    @Override
    void close();

    /**
     * Stop the execution of new tasks of this executor until the {@linkplain SystemExecutor system executor}
     * associated with this {@link SessionExecutor} executor executes job marked with the given value.
     * <p>
     * Task that had been added before this method has been called are not affected, even if they have not been executed
     * yet.
     * @param tick 
     * @see SystemExecutor#getTick() 
     */
    void pauseUntil(long tick);

    SessionTransaction createTransaction(TransactionMetainfo metainfo) throws ImplementationDbException;
    
    /**
     * 
     * @param collection
     * @param cursorId
     * @param queryCriteria
     * @param projection
     * @param maxResults 0 means no bounds
     * @return 
     */
    ListenableFuture<Void> query(
            @Nonnull String collection,
            @Nonnull CursorId cursorId,
            @Nullable QueryCriteria queryCriteria,
            @Nullable Projection projection,
            @Nonnegative int maxResults
    );
    
    ListenableFuture<FluentIterable<CollectionMetainfo>> getCollectionsMetainfo();

    /**
     *
     * @param cursorId
     * @param limit
     * @return a list of split documents retrieved from the cursor. Almost the given limit of documents are returned.
     *         less than the given limit of documents are returned iff the cursor reach its end.
     * @throws ToroTaskExecutionException
     */
    ListenableFuture<List<? extends SplitDocument>> readCursor(
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
    ListenableFuture<List<? extends SplitDocument>> readAllCursor(
            CursorId cursorId
    ) throws ToroTaskExecutionException;

    ListenableFuture<?> closeCursor(
            CursorId cursorId
    ) throws ToroTaskExecutionException;

    public ListenableFuture<Void> noop();
    
    public ListenableFuture<Integer> getMaxElements(CursorId cursorId);
}
