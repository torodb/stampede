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
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.index.NamedDbIndex;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaDocPart;

/**
 *
 */
@Singleton
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
            @Nonnull String databaseName
            ) {
    	String statement = getReadDatabaseSizeStatement(databaseName);
    	return sqlHelper.executeStatementWithResult(dsl, statement, Context.fetch)
    	        .get(0).into(Long.class);
    }

    protected abstract String getReadDatabaseSizeStatement(String databaseName);

    @Override
    public Long getCollectionSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schemaName,
            @Nonnull String collection
            ) {
        String statement = getReadCollectionSizeStatement(schemaName, collection);
        return sqlHelper.executeStatementWithResult(dsl, statement, Context.fetch)
                .get(0).into(Long.class);
    }

    protected abstract String getReadCollectionSizeStatement(String schema, String collection);

    @Override
    public Long getDocumentsSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection
            ) {
        String statement = getReadDocumentsSizeStatement(schema, collection);
        return sqlHelper.executeStatementWithResult(dsl, statement, Context.fetch)
                .get(0).into(Long.class);
    }

    protected abstract String getReadDocumentsSizeStatement(String schema, String collection);

    @Override
    public Long getIndexSize(
            @Nonnull DSLContext dsl,
            @Nonnull String schema,
            String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes,
            Map<String, Integer> relatedToroIndexes
            ) {
        long result = 0;
        for (NamedDbIndex dbIndex : relatedDbIndexes) {
            assert relatedToroIndexes.containsKey(dbIndex.getName());
            int usedBy = relatedToroIndexes.get(dbIndex.getName());
            assert usedBy != 0;
            String statement = getReadIndexSizeStatement(schema, collection, index, 
                    relatedDbIndexes, relatedToroIndexes);
            result += sqlHelper.executeStatementWithResult(dsl, statement, Context.fetch)
                    .get(0).into(Long.class) / usedBy;
        }
        return result;
    }

    protected abstract String getReadIndexSizeStatement(String schema,
            String collection, String index,
            Set<NamedDbIndex> relatedDbIndexes,
            Map<String, Integer> relatedToroIndexes);
    
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
}
