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

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

/**
 *
 */
public interface SessionTransaction extends Closeable {

    ListenableFuture<?> rollback();
    
    ListenableFuture<?> commit();
    
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
    ListenableFuture<InsertResponse> insertSplitDocuments(
            String collection,
            Collection<SplitDocument> splitDoc,
            WriteFailMode mode)
            throws ToroTaskExecutionException;

    public ListenableFuture<DeleteResponse> delete(
            @Nonnull String collection, 
            @Nonnull List<? extends DeleteOperation> deletes, 
            @Nonnull WriteFailMode mode
    );
    
    public ListenableFuture<NamedToroIndex> createIndex(
            @Nonnull String collection,
            @Nonnull String indexName, 
            @Nonnull IndexedAttributes attributes,
            boolean unique,
            boolean blocking
    );
    
    public ListenableFuture<Boolean> dropIndex(
            @Nonnull String collection,
            @Nonnull String indexName
    );
    
    public ListenableFuture<Collection<? extends NamedToroIndex>> getIndexes(
            @Nonnull String collection
    );

    public ListenableFuture<List<? extends Database>> getDatabases();

    public ListenableFuture<Integer> count(String collection, QueryCriteria query);

    public ListenableFuture<Long> getIndexSize(String collection, String indexName);
    
    public ListenableFuture<Long> getDocumentsSize(String collection);

    public ListenableFuture<Long> getCollectionSize(String collection);

    @Beta
    public ListenableFuture<Integer> createPathViews(String collection);

    @Beta
    public ListenableFuture<Void> dropPathViews(String collection);

    @Beta
    public ListenableFuture<Iterator<ValueRow<ScalarValue<?>>>> sqlSelect(String sqlQuery) throws UserToroException;
}
