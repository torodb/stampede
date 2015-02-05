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
import com.torodb.torod.core.exceptions.ExistentIndexException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.subdocument.ToroDocument;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

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
    
    public Future<NamedToroIndex> createIndex(
            @Nonnull String collection,
            @Nonnull String indexName, 
            @Nonnull IndexedAttributes attributes,
            boolean unique,
            boolean blocking
    ) throws ExistentIndexException;
    
    public Future<Boolean> dropIndex(
            @Nonnull String collection,
            @Nonnull String indexName
    );
    
    public Collection<? extends NamedToroIndex> getIndexes(@Nonnull String collection);
}
