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

package com.torodb.backend;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.lambda.tuple.Tuple2;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaFieldIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class AbstractMetaDataReadInterface implements MetaDataReadInterface {

    private final MetaDocPartTable<?, ?> metaDocPartTable;
    private final SqlHelper sqlHelper;

    @Inject
    public AbstractMetaDataReadInterface(MetaDocPartTable<?, ?> metaDocPartTable, SqlHelper sqlHelper) {
        this.metaDocPartTable = metaDocPartTable;
        this.sqlHelper = sqlHelper;
    }

    @Override
    public long getDatabaseSize(
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase database
            ) {
    	String statement = getReadSchemaSizeStatement(database.getIdentifier());
    	Result<Record> result = sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH, ps -> {
            ps.setString(1, database.getName());
        });
    	
    	if (result.isEmpty()) {
    	    return 0;
    	}
    	
    	Long resultSize = result.get(0).into(Long.class);
    	
    	if (resultSize == null) {
    	    return 0;
    	}
    	
    	return resultSize;
    }

    protected abstract String getReadSchemaSizeStatement(String databaseName);

    @Override
    public long getCollectionSize(
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase database,
            @Nonnull MetaCollection collection
            ) {
        String statement = getReadCollectionSizeStatement();
        return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH,
                ps -> {
                    ps.setString(1, database.getName());
                    ps.setString(2, database.getIdentifier());
                    ps.setString(3, collection.getName());
                }).get(0).into(Long.class);
    }

    protected abstract String getReadCollectionSizeStatement();

    @Override
    public long getDocumentsSize(
            @Nonnull DSLContext dsl,
            @Nonnull MetaDatabase database,
            @Nonnull MetaCollection collection
            ) {
        String statement = getReadDocumentsSizeStatement();
        return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH,
                ps -> {
                    ps.setString(1, database.getName());
                    ps.setString(2, database.getIdentifier());
                    ps.setString(3, collection.getName());
                }).get(0).into(Long.class);
    }

    protected abstract String getReadDocumentsSizeStatement();

    @Override
    public Long getIndexSize(
            @Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
            @Nonnull MetaCollection collection, @Nonnull String index) {
        long result = 0;
        Iterator<Tuple2<MetaDocPart, MetaDocPartIndex>> docPartIndexIterator = streamMetaDocPartIndexesByIndexName(collection, index).iterator();
        while (docPartIndexIterator.hasNext()) {
            Tuple2<MetaDocPart, MetaDocPartIndex> indexEntry = docPartIndexIterator.next();
            long relatedIndexCount = streamMetaIndexesByIndexIdentifier(collection, indexEntry.v2().getIdentifier()).count();
            String statement = getReadIndexSizeStatement(database.getIdentifier(), 
                    indexEntry.v1().getIdentifier(), indexEntry.v2().getIdentifier());
            result += sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH)
                    .get(0).into(Long.class) / relatedIndexCount;
        }
        return result;
    }

    private Stream<Tuple2<MetaDocPart, MetaDocPartIndex>> streamMetaDocPartIndexesByIndexName(MetaCollection collection, String indexName) {
        MetaIndex metaIndex = collection.getMetaIndexByName(indexName);
        return collection.streamContainedMetaDocParts()
                .flatMap(docPart -> docPart.streamIndexes().map(docPartIndex -> new Tuple2<MetaDocPart, MetaDocPartIndex>(docPart, docPartIndex)))
                .filter(t -> t.v1().streamIndexes()
                        .flatMap(docPartIndex -> docPartIndex.streamFields())
                                .allMatch(fieldIndex -> matchIndexField(metaIndex, t.v1(), fieldIndex)));
    }
    
    private boolean matchIndexField(MetaIndex metaIndex, MetaDocPart docPart, MetaFieldIndex fieldIndex) {
        MetaIndexField indexField = metaIndex.getMetaIndexFieldByPosition(fieldIndex.getPosition());
        return indexField.getTableRef().equals(docPart.getTableRef()) &&
                indexField.getName().equals(fieldIndex.getName()) && 
                indexField.getOrdering().equals(fieldIndex.getOrdering());
    }

    private Stream<MetaIndex> streamMetaIndexesByIndexIdentifier(MetaCollection collection, String indexIdentifier) {
        Tuple2<MetaDocPart, MetaDocPartIndex> metaDocPartIndex = collection.streamContainedMetaDocParts()
                .flatMap(docPart -> docPart.streamIndexes().map(docPartIndex -> new Tuple2<MetaDocPart, MetaDocPartIndex>(docPart, docPartIndex)))
                .filter(docPart -> docPart.v1().getMetaDocPartIndexByIdentifier(indexIdentifier) != null)
                .findAny().get();
        return collection.streamContainedMetaIndexes()
                .filter(index -> index.streamFields()
                        .allMatch(indexField -> matchFieldIndex(metaDocPartIndex.v1(), metaDocPartIndex.v2(), indexField)))
                .map(index -> (MetaIndex) index);
    }
    
    private boolean matchFieldIndex(MetaDocPart docPart, MetaDocPartIndex docPartIndex, MetaIndexField indexField) {
        MetaFieldIndex fieldIndex = docPartIndex.getMetaFieldIndexByPosition(indexField.getPosition());
        return docPart.getTableRef().equals(indexField.getTableRef()) &&
                fieldIndex.getName().equals(indexField.getName()) && 
                fieldIndex.getOrdering().equals(indexField.getOrdering());
    }

    protected abstract String getReadIndexSizeStatement(
            String schemaName, String tableName, String indexName);
    
    @Override
    public Collection<InternalField<?>> getInternalFields(MetaDocPart metaDocPart) {
        TableRef tableRef = metaDocPart.getTableRef();
        return getInternalFields(tableRef);
    }

    @Override
    public Collection<InternalField<?>> getInternalFields(TableRef tableRef) {
        if (tableRef.isRoot()) {
            return metaDocPartTable.ROOT_FIELDS;
        } else if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.FIRST_FIELDS;
        }
        return metaDocPartTable.FIELDS;
    }

    @Override
    public Collection<InternalField<?>> getPrimaryKeyInternalFields(TableRef tableRef) {
        if (tableRef.isRoot()) {
            return metaDocPartTable.PRIMARY_KEY_ROOT_FIELDS;
        } else if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.PRIMARY_KEY_FIRST_FIELDS;
        }
        return metaDocPartTable.PRIMARY_KEY_FIELDS;
    }

    @Override
    public Collection<InternalField<?>> getReferenceInternalFields(TableRef tableRef) {
        Preconditions.checkArgument(!tableRef.isRoot());
        if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.REFERENCE_FIRST_FIELDS;
        }
        return metaDocPartTable.REFERENCE_FIELDS;
    }

    @Override
    public Collection<InternalField<?>> getForeignInternalFields(TableRef tableRef) {
        Preconditions.checkArgument(!tableRef.isRoot());
        TableRef parentTableRef = tableRef.getParent().get();
        if (parentTableRef.isRoot()) {
            return metaDocPartTable.FOREIGN_ROOT_FIELDS;
        } else if (parentTableRef.getParent().get().isRoot()) {
            return metaDocPartTable.FOREIGN_FIRST_FIELDS;
        }
        return metaDocPartTable.FOREIGN_FIELDS;
    }

    @Override
    public Collection<InternalField<?>> getReadInternalFields(MetaDocPart metaDocPart) {
        TableRef tableRef = metaDocPart.getTableRef();
        return getReadInternalFields(tableRef);
    }

    @Override
    public Collection<InternalField<?>> getReadInternalFields(TableRef tableRef) {
        if (tableRef.isRoot()) {
            return metaDocPartTable.READ_ROOT_FIELDS;
        } else if (tableRef.getParent().get().isRoot()) {
            return metaDocPartTable.READ_FIRST_FIELDS;
        }
        return metaDocPartTable.READ_FIELDS;
    }
}
