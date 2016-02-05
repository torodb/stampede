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

import com.google.common.annotations.Beta;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;

/**
 *
 */
public interface ToroTransaction extends AutoCloseable {

    public ListenableFuture<?> rollback();

    public ListenableFuture<?> commit();

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
    public ListenableFuture<InsertResponse> insertDocuments(
            @Nonnull String collection,
            @Nonnull FluentIterable<ToroDocument> documents,
            WriteFailMode mode
    );
    
    /**
     * Deletes documents that fulfil the given condition.
     * <p>
     * @param collection
     * @param deletes
     * @param mode
     * @return A {@linkplain Future future} that can be used to wait until the
     *         action is commited.
     */
    public ListenableFuture<DeleteResponse> delete(
            @Nonnull String collection,
            @Nonnull List<? extends DeleteOperation> deletes,
            @Nonnull WriteFailMode mode
    );

    public ListenableFuture<UpdateResponse> update(
            @Nonnull String collection,
            @Nonnull List<? extends UpdateOperation> updates,
            @Nonnull WriteFailMode mode
    );
    
    public ListenableFuture<NamedToroIndex> createIndex(
            @Nonnull String collection,
            @Nonnull String indexName, 
            @Nonnull IndexedAttributes attributes,
            boolean unique,
            boolean blocking
    ) throws ExistentIndexException;
    
    public ListenableFuture<Boolean> dropIndex(
            @Nonnull String collection,
            @Nonnull String indexName
    );
    
    public Collection<? extends NamedToroIndex> getIndexes(@Nonnull String collection);

    /**
     * 
     * @param collection
     * @param indexName
     * @return the bytes used to store indexes
     */
    public ListenableFuture<Long> getIndexSize(String collection, String indexName);

    /**
     * 
     * @param collection
     * @return the bytes used to store a collection. It includes its documents,
     * indexes and metadata
     */
    public ListenableFuture<Long> getCollectionSize(String collection);
    
    /**
     * 
     * @param collection
     * @return the bytes used to store all documents in a collection. It does
     * not include indexes or metadata
     */
    public ListenableFuture<Long> getDocumentsSize(String collection);

    public ListenableFuture<Integer> count(String collection, QueryCriteria query);

    public ListenableFuture<List<? extends Database>> getDatabases();

    @Beta
    public ListenableFuture<Integer> createPathViews(String collection) throws UnsupportedOperationException;

    @Beta
    public ListenableFuture<Void> dropPathViews(String collection) throws UnsupportedOperationException;

    @Beta
    public ListenableFuture<Iterator<ValueRow<KVValue<?>>>> sqlSelect(String sqlQuery) throws UnsupportedOperationException, UserToroException;
}
