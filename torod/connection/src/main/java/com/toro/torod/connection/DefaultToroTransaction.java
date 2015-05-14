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

package com.toro.torod.connection;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.toro.torod.connection.update.Updator;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.*;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.exceptions.*;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.EqualFactory;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import java.util.Collection;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class DefaultToroTransaction implements ToroTransaction {

    private final SessionTransaction sessionTransaction;
    private final D2RTranslator d2r;
    private final SessionExecutor executor;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final DefaultToroConnection connection;
    
    DefaultToroTransaction(
            DefaultToroConnection connection,
            SessionTransaction sessionTransaction,
            D2RTranslator d2r,
            SessionExecutor executor,
            DocumentBuilderFactory documentBuilderFactory) {
        this.connection = connection;
        this.sessionTransaction = sessionTransaction;
        this.d2r = d2r;
        this.executor = executor;
        this.documentBuilderFactory = documentBuilderFactory;
    }

    @Override
    public void close() {
        sessionTransaction.close();
    }

    @Override
    public Future<?> rollback() {
        return sessionTransaction.rollback();
    }

    @Override
    public Future<?> commit() {
        return sessionTransaction.commit();
    }

    @Override
    public Future<InsertResponse> insertDocuments(
            String collection,
            Iterable<ToroDocument> documents,
            WriteFailMode mode
    ) {

        try {
            List<SplitDocument> documentsList = Lists.newArrayList();

            for (ToroDocument document : documents) {
                SplitDocument splitDoc
                        = d2r.translate(executor, collection, document);
                documentsList.add(splitDoc);
            }

            return sessionTransaction.insertSplitDocuments(
                    collection,
                    documentsList,
                    mode
            );

        }
        catch (ToroTaskExecutionException ex) {
            //TODO: Change exceptions
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Future<DeleteResponse> delete(
            @Nonnull String collection,
            @Nonnull List<? extends DeleteOperation> deletes,
            @Nonnull WriteFailMode mode) {
        return sessionTransaction.delete(collection, deletes, mode);
    }

    @Override
    public Future<NamedToroIndex> createIndex(
            String collection,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        return sessionTransaction.createIndex(
                collection, 
                indexName, 
                attributes, 
                unique, 
                blocking
        );
    }

    @Override
    public Future<Boolean> dropIndex(String collection, String indexName) {
        return sessionTransaction.dropIndex(collection, indexName);
    }

    @Override
    public Collection<? extends NamedToroIndex> getIndexes(String collection) {
        try {
            return sessionTransaction.getIndexes(collection).get();
        }
        catch (InterruptedException ex) {
            throw new ToroRuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public Future<Long> getIndexSize(String collection, String indexName) {
        return sessionTransaction.getIndexSize(collection, indexName);
    }

    @Override
    public Future<Long> getCollectionSize(String collection) {
        return sessionTransaction.getCollectionSize(collection);
    }

    @Override
    public Future<Long> getDocumentsSize(String collection) {
        return sessionTransaction.getDocumentsSize(collection);
    }

    @Override
    public Future<Integer> count(String collection, QueryCriteria query) {
        return sessionTransaction.count(collection, query);
    }
    
    @Override
    public Future<List<? extends Database>> getDatabases() {
        return sessionTransaction.getDatabases();
    }

    @Override
    public Future<UpdateResponse> update(
            String collection,
            List<? extends UpdateOperation> updates,
            WriteFailMode mode
    ) {
        for (UpdateOperation update : updates) {
            if (update.isInsertIfNotFound()) {
                throw new UserToroException("Upsert updates are not supported");
            }
        }

        UpdateResponse.Builder builder = new UpdateResponse.Builder();
        for (int i = 0; i < updates.size(); i++) {
            UpdateOperation update = updates.get(i);

            try {
                update(collection, update, builder);
            }
            catch (InterruptedException ex) {
                throw new ToroImplementationException(ex);
            }
            catch (ExecutionException ex) {
                throw new ToroImplementationException(ex);
            }
            catch (UserToroException ex) {
                builder.addError(
                        new WriteError(
                                i,
                                -1,
                                ex.getLocalizedMessage()
                        )
                );
            }
        }

        return Futures.immediateCheckedFuture(builder.build());
    }

    private void update(
            String collection,
            UpdateOperation update,
            UpdateResponse.Builder responseBuilder
    ) throws InterruptedException, ExecutionException {

        UserCursor<ToroDocument> cursor;
        try {
            cursor = connection.openUnlimitedCursor(
                            collection,
                            update.getQuery(),
                            null,
                            0,
                            true,
                            false
                    );
        }
        catch (NotAutoclosableCursorException ex) {
            throw new ToroImplementationException("This should not happen!", ex);
        }
        List<ToroDocument> candidates;
        try {
            candidates = cursor.readAll();
        }
        catch (ClosedToroCursorException ex) {
            throw new ToroImplementationException(
                    "The used cursor has been closed before it can be used", 
                    ex
            );
        }

        responseBuilder.addCandidates(candidates.size());

        if (candidates.isEmpty()) {
            if (update.isInsertIfNotFound()) {
                ToroDocument documentToInsert = documentToInsert(update.
                        getAction());
                Future<InsertResponse> insertFuture
                        = insertDocuments(collection, Collections.singleton(
                                                  documentToInsert),
                                          WriteFailMode.TRANSACTIONAL);
                //as we are using a synchronized update, we need to wait until the insert is executed
                insertFuture.get();
            }
        }
        Set<ToroDocument> objectsToDelete = Sets.newHashSet();
        Set<ToroDocument> objectsToInsert = Sets.newHashSet();
        for (ToroDocument candidate : candidates) {
            ToroDocument newDoc = Updator.update(
                    candidate,
                    update.getAction(),
                    responseBuilder,
                    documentBuilderFactory
            );
            if (newDoc != null) {
                objectsToDelete.add(candidate);
                objectsToInsert.add(newDoc);

                if (update.isJustOne()) {
                    break;
                }
            }
        }
        if (!objectsToDelete.isEmpty()) {
            List<DeleteOperation> deletes = Lists.newArrayListWithCapacity(
                    objectsToDelete.size()
            );
            for (ToroDocument objectToDelete : objectsToDelete) {
                deletes.add(
                        new DeleteOperation(
                                EqualFactory.createEquality(objectToDelete),
                                true
                        )
                );
            }
            DeleteResponse deleteResponse
                    = delete(
                            collection,
                            deletes,
                            WriteFailMode.TRANSACTIONAL
                    ).get();
            if (deleteResponse.getDeleted() != objectsToDelete.size()) {
                throw new ToroImplementationException("Update: "
                        + objectsToDelete.size() + " should be deleted, but "
                        + deleteResponse.getDeleted() + " objects have been "
                        + "deleted instead");
            }
        }
        if (!objectsToInsert.isEmpty()) {
            InsertResponse insertResponse
                    = insertDocuments(
                            collection,
                            objectsToInsert,
                            WriteFailMode.TRANSACTIONAL
                    ).get();
            if (insertResponse.getInsertedSize() != objectsToInsert.size()) {
                throw new ToroImplementationException("Update: "
                        + objectsToInsert.size() + " should be inserted, but "
                        + insertResponse.getInsertedSize() + " objects have "
                        + "been inserted instead");
            }
        }

    }

    private ToroDocument documentToInsert(UpdateAction action) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
