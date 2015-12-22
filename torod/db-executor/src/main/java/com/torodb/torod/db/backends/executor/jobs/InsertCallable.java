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

package com.torodb.torod.db.backends.executor.jobs;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.WriteError;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.DbException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.SubDocument;
import java.util.*;
import javax.annotation.Nonnull;

/**
 *
 */
public class InsertCallable extends TransactionalJob<InsertResponse> {

    private final String collection;
    private final Collection<SplitDocument> docs;
    private final WriteFailMode mode;
    private final Report report;

    public InsertCallable(
            DbConnection connection, 
            TransactionAborter abortCallback, 
            Report report, 
            String collection, 
            Collection<SplitDocument> docs, 
            WriteFailMode mode) {
        super(connection, abortCallback);
        this.collection = collection;
        this.docs = docs;
        this.mode = mode;
        this.report = report;
    }

    @Override
    protected InsertResponse failableCall() throws ToroException, ToroRuntimeException {
        try {
            InsertResponse result;
            switch (mode) {
                case ISOLATED:
                    result = isolatedInsert();
                    break;
                case ORDERED:
                    result = orderedInsert();
                    break;
                case TRANSACTIONAL:
                    result = transactionalInsert();
                    break;
                default:
                    throw new AssertionError("Study exceptions");
            }
            report.insertExecuted(collection, docs, mode, result);
            
            return result;
        }
        catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
    }

    private void insertSingleDoc(SplitDocument doc) throws ImplementationDbException, UserDbException {
        DbConnection connection = getConnection();
        connection.insertRootDocuments(collection, Collections.singleton(doc));
        
        for (SubDocType type : doc.getSubDocuments().rowKeySet()) {
            ImmutableCollection<SubDocument> subDocs = doc.getSubDocuments().row(type).values();

            connection.insertSubdocuments(collection, type, subDocs.iterator());
        }
    }

    private InsertResponse isolatedInsert() throws ImplementationDbException {
        int index = 0;
        List<WriteError> errors = Lists.newLinkedList();
        for (SplitDocument doc : docs) {
            try {
                insertSingleDoc(doc);

                index++;
            } catch (UserDbException ex) {
                appendError(errors, ex, index);
            }
        }

        return createResponse(docs.size(), errors);
    }

    private InsertResponse orderedInsert() throws ImplementationDbException {
        int index = 0;
        List<WriteError> errors = Lists.newLinkedList();
        try {
            for (SplitDocument doc : docs) {
                insertSingleDoc(doc);

                index++;
            }
        } catch (UserDbException ex) {
            appendError(errors, ex, index);
        }

        return createResponse(docs.size(), errors);
    }

    private InsertResponse transactionalInsert() throws ImplementationDbException {
        DbConnection connection = getConnection();
        List<WriteError> errors = Lists.newLinkedList();
        
        try {
            /*
            * First we need to store the root documents
            */
            connection.insertRootDocuments(collection, docs);
            
            /*
            * Then we have to store the subdocuments. It is more efficient to do one insert for each table, so inserts
            * are done by subdocument type.
            * To do that, we could create a map like Map<SubDocType, List<SubDocument>> and then iterate over the keys,
            * but we need to duplicate memory and the documents to insert may be very big. So we decided to do it in a
            * functional way. First we get all types and then we use an iterator that, for each type 't' and document 'd'
            * does d.getSubDocuments().row(k).values().iterator and finally merges the iterators grouped by type.
            */
            Set<SubDocType> types = Sets.newHashSetWithExpectedSize(10 * docs.size());
            for (SplitDocument splitDocument : docs) {
                types.addAll(splitDocument.getSubDocuments().rowKeySet());
            }
            
            /*
            * The following code that uses guava functions is the same as the following jdk8 code:
            * for (SubDocType type : types) {
            *   java.util.function.Function<SplitDocument, Stream<SubDocument>> f = (sd) -> sd.getSubDocuments().row(type).values().stream();
            *
            *   Stream<SubDocument> flatMap = docs.stream().map(f).flatMap((stream) -> stream);
            *
            *   connection.insertSubdocuments(collection, type, flatMap.iterator());
            *
            * }
            */
            for (SubDocType type : types) {
                Function<SplitDocument, Iterable<SubDocument>> extractor = new SubDocumentExtractorFunction(type);
                
                connection.insertSubdocuments(collection, type, Iterables.concat(Iterables.transform(docs, extractor)).iterator());
            }
            
            return createResponse(docs.size(), errors);
        } catch (UserDbException ex) {
            appendError(errors, ex, 0);
            connection.rollback();
        }
        return createResponse(0, errors);
    }

    private InsertResponse createResponse(int docInserted, @Nonnull List<WriteError> errors) {
        if (errors.isEmpty()) {
            assert docInserted == docs.size();
            return new InsertResponse(true, docInserted, null);
        }
        return new InsertResponse(false, docInserted, ImmutableList.copyOf(errors));
    }

    private void appendError(@Nonnull List<WriteError> errors, DbException ex, int index) {
        errors.add(new WriteError(index, getErrorCode(ex), getErrorMessage(ex)));
    }

    private int getErrorCode(DbException ex) {
        return -1;
    }

    private String getErrorMessage(DbException ex) {
        return ex.getMessage();
    }

    /**
     * This function extracts the subdocuments of a given type contained in a given
     * {@linkplain SubDocument subdocuments}.
     */
    private static class SubDocumentExtractorFunction implements Function<SplitDocument, Iterable<SubDocument>> {

        private final SubDocType type;

        public SubDocumentExtractorFunction(SubDocType type) {
            this.type = type;
        }

        @Override
        public Iterable<SubDocument> apply(SplitDocument input) {
            if (input == null) {
                throw new IllegalArgumentException();
            }
            return input.getSubDocuments().row(type).values();
        }
    }
    
    public static interface Report {
        public void insertExecuted(
                String collection,
                Collection<SplitDocument> docs,
                WriteFailMode mode,
                InsertResponse response
        );
    }
}
