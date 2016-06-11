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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ByteArrayKVMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteSourceKVBinary;
import com.torodb.kvdocument.values.heap.DefaultKVMongoTimestamp;
import com.torodb.kvdocument.values.heap.ListKVArray;
import com.torodb.kvdocument.values.heap.LocalDateKVDate;
import com.torodb.kvdocument.values.heap.LocalTimeKVTime;
import com.torodb.kvdocument.values.heap.LongKVInstant;
import com.torodb.kvdocument.values.heap.StringKVString;
import com.torodb.torod.core.ValueRow;
import com.torodb.torod.core.ValueRow.TranslatorValueRow;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.UpdateResponse;
import com.torodb.torod.core.connection.WriteError;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.d2r.D2RTranslator;
import com.torodb.torod.core.dbMetaInf.DbMetaInformationCache;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.executor.SessionExecutor;
import com.torodb.torod.core.executor.SessionTransaction;
import com.torodb.torod.core.executor.ToroTaskExecutionException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.utils.EqualFactory;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitor;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.IndexedAttributes;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.ScalarBoolean;
import com.torodb.torod.core.subdocument.values.ScalarDate;
import com.torodb.torod.core.subdocument.values.ScalarDouble;
import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.subdocument.values.ScalarLong;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarString;
import com.torodb.torod.core.subdocument.values.ScalarTime;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.core.subdocument.values.ScalarValueVisitor;

/**
 *
 */
public class DefaultToroTransaction implements ToroTransaction {

    private final DbMetaInformationCache cache;
    private final SessionTransaction sessionTransaction;
    private final D2RTranslator d2r;
    private final SessionExecutor executor;
    
    DefaultToroTransaction(
            DbMetaInformationCache cache,
            SessionTransaction sessionTransaction,
            D2RTranslator d2r,
            SessionExecutor executor) {
        this.cache = cache;
        this.sessionTransaction = sessionTransaction;
        this.d2r = d2r;
        this.executor = executor;
    }

    @Override
    public void close() {
        sessionTransaction.close();
    }

    @Override
    public ListenableFuture<?> rollback() {
        return sessionTransaction.rollback();
    }

    @Override
    public ListenableFuture<?> commit() {
        return sessionTransaction.commit();
    }

    @Override
    public ListenableFuture<InsertResponse> insertDocuments(
            String collection,
            FluentIterable<ToroDocument> documents,
            WriteFailMode mode
    ) {

        try {

            List<SplitDocument> documentsList = Lists.newArrayList();

            Function<ToroDocument, SplitDocument> toRelationFun
                    = d2r.getToRelationalFunction(executor, collection);
            for (ToroDocument document : documents) {
                documentsList.add(toRelationFun.apply(document));
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
    public ListenableFuture<DeleteResponse> delete(
            @Nonnull String collection,
            @Nonnull List<? extends DeleteOperation> deletes,
            @Nonnull WriteFailMode mode) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.delete(collection, deletes, mode);
    }

    @Override
    public ListenableFuture<NamedToroIndex> createIndex(
            String collection,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.createIndex(
                collection, 
                indexName, 
                attributes, 
                unique, 
                blocking
        );
    }

    @Override
    public ListenableFuture<Boolean> dropIndex(String collection, String indexName) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.dropIndex(collection, indexName);
    }

    @Override
    public Collection<? extends NamedToroIndex> getIndexes(String collection) {
        try {
            cache.createCollection(executor, collection, null);
            return sessionTransaction.getIndexes(collection).get();
        }
        catch (InterruptedException | ExecutionException ex) {
            throw new ToroRuntimeException(ex);
        }
    }

    @Override
    public ListenableFuture<Long> getIndexSize(String collection, String indexName) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.getIndexSize(collection, indexName);
    }

    @Override
    public ListenableFuture<Long> getCollectionSize(String collection) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.getCollectionSize(collection);
    }

    @Override
    public ListenableFuture<Long> getDocumentsSize(String collection) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.getDocumentsSize(collection);
    }

    @Override
    public ListenableFuture<Integer> count(String collection, QueryCriteria query) {
        cache.createCollection(executor, collection, null);
        return sessionTransaction.count(collection, query);
    }
    
    @Override
    public ListenableFuture<List<? extends Database>> getDatabases() {
        return sessionTransaction.getDatabases();
    }

    @Override
    public ListenableFuture<Integer> createPathViews(String collection) throws UnsupportedOperationException {
        return sessionTransaction.createPathViews(collection);
    }

    @Override
    public ListenableFuture<Void> dropPathViews(String collection) throws
            UnsupportedOperationException {
        return sessionTransaction.dropPathViews(collection);
    }

    @Override
    public ListenableFuture<Iterator<ValueRow<KVValue<?>>>> sqlSelect(String sqlQuery) throws
            UnsupportedOperationException, UserToroException {
        ListenableFuture<Iterator<ValueRow<ScalarValue<?>>>> valueFuture
                = sessionTransaction.sqlSelect(sqlQuery);

        IteratorTranslator<ValueRow<ScalarValue<?>>, ValueRow<KVValue<?>>> iteratorTranslator
                = new IteratorTranslator<>(
                        TranslatorValueRow.getBuilderFunction(new ToDocValueFunction())
                );

        return Futures.transform(
                valueFuture,
                iteratorTranslator,
                MoreExecutors.directExecutor()
        );
    }

    @Override
    public ListenableFuture<UpdateResponse> update(
            String collection,
            List<? extends UpdateOperation> updates,
            WriteFailMode mode,
            UpdateActionVisitor<ToroDocument, Void> updateActionVisitorDocumentToInsert,
            UpdateActionVisitor<UpdatedToroDocument, ToroDocument> updateActionVisitorDocumentToUpdate
    ) {
        cache.createCollection(executor, collection, null);
        UpdateResponse.Builder builder = new UpdateResponse.Builder();
        for (int updateIndex = 0; updateIndex < updates.size(); updateIndex++) {
            UpdateOperation update = updates.get(updateIndex);
            
            try {
                update(
                        collection, 
                        update, 
                        updateIndex, 
                        updateActionVisitorDocumentToInsert, 
                        updateActionVisitorDocumentToUpdate,
                        builder);
            } catch (InterruptedException | ExecutionException | RetryTransactionException ex) {
                return Futures.immediateFailedCheckedFuture(ex);
            }
            catch (UserToroException ex) {
                builder.addError(
                        new WriteError(
                                updateIndex,
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
            int updateIndex,
            UpdateActionVisitor<ToroDocument, Void> updateActionVisitorDocumentToInsert,
            UpdateActionVisitor<UpdatedToroDocument, ToroDocument> updateActionVisitorDocumentToUpdate,
            UpdateResponse.Builder responseBuilder
    ) throws InterruptedException, ExecutionException, RetryTransactionException {
        List<ToroDocument> candidates = sessionTransaction.readAll(collection, update.getQuery()).get();
        
        responseBuilder.addCandidates(candidates.size());

        if (candidates.isEmpty() && update.isInsertIfNotFound()) {
            ToroDocument documentToInsert = documentToInsert(update.
                    getAction(), updateActionVisitorDocumentToInsert);
            Future<InsertResponse> insertFuture = insertDocuments(
                    collection,
                    FluentIterable.from(
                            Collections.singleton(
                                    documentToInsert
                            )
                    ),
                    WriteFailMode.TRANSACTIONAL
            );
            //as we are using a synchronized update, we need to wait until the insert is executed
            insertFuture.get();
            responseBuilder.addInsertedDocument(documentToInsert, updateIndex);
            responseBuilder.addCandidates(1);
        }
        Set<ToroDocument> objectsToDelete = Sets.newHashSet();
        Set<ToroDocument> objectsToInsert = Sets.newHashSet();
        for (ToroDocument candidate : candidates) {
            UpdatedToroDocument documentUpdated = documentUpdated(update.getAction(), candidate, updateActionVisitorDocumentToUpdate);
            if (documentUpdated.isUpdated()) {
                responseBuilder.incrementModified(1);

                objectsToDelete.add(candidate);
                objectsToInsert.add(documentUpdated);

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
                throw new RetryTransactionException("Update: "
                        + objectsToDelete.size() + " should be deleted, but "
                        + deleteResponse.getDeleted() + " objects have been "
                        + "deleted instead");
            }
        }
        if (!objectsToInsert.isEmpty()) {
            InsertResponse insertResponse
                    = insertDocuments(
                            collection,
                            FluentIterable.from(objectsToInsert),
                            WriteFailMode.TRANSACTIONAL
                    ).get();
            if (insertResponse.getInsertedDocsCounter() != objectsToInsert.size()) {
                throw new ToroImplementationException("Update: "
                        + objectsToInsert.size() + " should be inserted, but "
                        + insertResponse.getInsertedDocsCounter() + " objects have "
                        + "been inserted instead");
            }
        }

    }

    private ToroDocument documentToInsert(UpdateAction action, UpdateActionVisitor<ToroDocument, Void> updateActionVisitorDocumentToInsert) {
        return action.accept(updateActionVisitorDocumentToInsert, null);
    }

    private UpdatedToroDocument documentUpdated(UpdateAction action, ToroDocument candidate, UpdateActionVisitor<UpdatedToroDocument, ToroDocument> updateActionVisitorDocumentToUpdate) {
        return action.accept(updateActionVisitorDocumentToUpdate, candidate);
    }

    private static class IteratorTranslator<I,O> implements Function<Iterator<I>, Iterator<O>> {

        private final Function<I, O> function;

        private IteratorTranslator(Function<I, O> function) {
            this.function = function;
        }

        @Override
        public Iterator<O> apply(@Nonnull Iterator<I> input) {
            return Iterators.transform(input, function);
        }
    }

    private static class ToDocValueFunction implements Function<ScalarValue<?>, KVValue<?>>, ScalarValueVisitor<KVValue<?>, Void> {

        @Override
        public KVValue<?> apply(@Nonnull ScalarValue<?> input) {
            return (KVValue<?>) input.accept(this, null);
        }

        @Override
        public KVValue<?> visit(ScalarBoolean value, Void arg) {
            if (value.getValue()) {
                return KVBoolean.TRUE;
            }
            return KVBoolean.FALSE;
        }

        @Override
        public KVValue<?> visit(ScalarNull value, Void arg) {
            return KVNull.getInstance();
        }

        @Override
        public KVValue<?> visit(ScalarArray value, Void arg) {
            return new ListKVArray(Lists.newArrayList(Iterators.transform(value.iterator(), this)));
        }

        @Override
        public KVValue<?> visit(ScalarInteger value, Void arg) {
            return KVInteger.of(value.getValue());
        }

        @Override
        public KVValue<?> visit(ScalarLong value, Void arg) {
            return KVLong.of(value.longValue());
        }

        @Override
        public KVValue<?> visit(ScalarDouble value, Void arg) {
            return KVDouble.of(value.doubleValue());
        }

        @Override
        public KVValue<?> visit(ScalarString value, Void arg) {
            return new StringKVString(value.getValue());
        }

        @Override
        public KVValue<?> visit(ScalarMongoObjectId value, Void arg) {
            return new ByteArrayKVMongoObjectId(value.getArrayValue());
        }

        @Override
        public KVValue<?> visit(ScalarMongoTimestamp value, Void arg) {
            return new DefaultKVMongoTimestamp(value.getSecondsSinceEpoch(), value.getOrdinal());
        }

        @Override
        public KVValue<?> visit(ScalarInstant value, Void arg) {
            return new LongKVInstant(value.getMillisFromUnix());
        }

        @Override
        public KVValue<?> visit(ScalarDate value, Void arg) {
            return new LocalDateKVDate(value.getValue());
        }

        @Override
        public KVValue<?> visit(ScalarTime value, Void arg) {
            return new LocalTimeKVTime(value.getValue());
        }

        @Override
        public KVValue<?> visit(ScalarBinary value, Void arg) {
            return new ByteSourceKVBinary(value.getSubtype(), value.getCategory(), value.getByteSource());
        }

    }
}
