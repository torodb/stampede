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
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;

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

    public Future<DeleteResponse> delete(
            @Nonnull String collection, 
            @Nonnull List<? extends DeleteOperation> deletes, 
            @Nonnull WriteFailMode mode
    );
    
    public Future<NamedToroIndex> createIndex(
            @Nonnull String collection,
            @Nonnull String indexName, 
            @Nonnull IndexedAttributes attributes,
            boolean unique,
            boolean blocking
    );
    
    public Future<Boolean> dropIndex(
            @Nonnull String collection,
            @Nonnull String indexName
    );
    
    public Future<Collection<? extends NamedToroIndex>> getIndexes(
            @Nonnull String collection
    );

    public Future<List<? extends Database>> getDatabases();

    public Future<Integer> count(String collection, QueryCriteria query);

    public Future<Long> getIndexSize(String collection, String indexName);
    
    public Future<Long> getDocumentsSize(String collection);

    public Future<Long> getCollectionSize(String collection);
}
