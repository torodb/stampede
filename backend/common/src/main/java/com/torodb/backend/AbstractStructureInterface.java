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

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

/**
 *
 */
@Singleton
public abstract class AbstractStructureInterface implements StructureInterface {

    private final MetaDataReadInterface metaDataReadInterface;
    private final SqlHelper sqlHelper;
    
    @Inject
    public AbstractStructureInterface(MetaDataReadInterface metaDataReadInterface, SqlHelper sqlHelper) {
        this.metaDataReadInterface = metaDataReadInterface;
        this.sqlHelper = sqlHelper;
    }

    @Override
    public void dropDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase metaDatabase) {
        Iterator<? extends MetaCollection> metaCollectionIterator = metaDatabase.streamMetaCollections()
                .iterator();
        while (metaCollectionIterator.hasNext()) {
            MetaCollection metaCollection = metaCollectionIterator.next();
            Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection.streamContainedMetaDocParts()
                    .sorted(TableRefComparator.MetaDocPart.DESC).iterator();
            while (metaDocPartIterator.hasNext()) {
                MetaDocPart metaDocPart = metaDocPartIterator.next();
                String statement = getDropTableStatement(metaDatabase.getIdentifier(), metaDocPart.getIdentifier());
                sqlHelper.executeUpdate(dsl, statement, Context.DROP_TABLE);
            }
        }
        String statement = getDropSchemaStatement(metaDatabase.getIdentifier());
        sqlHelper.executeUpdate(dsl, statement, Context.DROP_SCHEMA);
    }

    @Override
    public void dropCollection(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull MetaCollection metaCollection) {
        Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection.streamContainedMetaDocParts()
                .sorted(TableRefComparator.MetaDocPart.DESC).iterator();
        while (metaDocPartIterator.hasNext()) {
            MetaDocPart metaDocPart = metaDocPartIterator.next();
            String statement = getDropTableStatement(schemaName, metaDocPart.getIdentifier());
            sqlHelper.executeUpdate(dsl, statement, Context.DROP_TABLE);
        }
    }

    protected abstract String getDropTableStatement(String schemaName, String tableName);

    protected abstract String getDropSchemaStatement(String schemaName);
    
    @Override
    public void createIndex(@Nonnull DSLContext dsl,
            @Nonnull String indexName, @Nonnull String schemaName, @Nonnull String tableName,
            @Nonnull String columnName, boolean ascending
    ) {
        String statement = getCreateIndexStatement(indexName, schemaName, tableName, columnName, ascending);

        sqlHelper.executeUpdate(dsl, statement, Context.CREATE_INDEX);
    }

    protected abstract String getCreateIndexStatement(String indexName, String schemaName, String tableName, String columnName,
            boolean ascending);
    
    @Override
    public void dropIndex(@Nonnull DSLContext dsl, @Nonnull String schemaName, @Nonnull String indexName) {
        String statement = getDropIndexStatement(schemaName, indexName);
        
        sqlHelper.executeUpdate(dsl, statement, Context.DROP_INDEX);
    }

    protected String getDropIndexStatement(String schemaName, String indexName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP INDEX ")
                .append("\"").append(schemaName).append("\"")
                .append(".")
                .append("\"").append(indexName).append("\"");
        String statement = sb.toString();
        return statement;
    }

    @Override
    public void createSchema(@Nonnull DSLContext dsl, @Nonnull String schemaName){
    	String statement = getCreateSchemaStatement(schemaName);
    	sqlHelper.executeUpdate(dsl, statement, Context.CREATE_SCHEMA);
    }

    protected abstract String getCreateSchemaStatement(String schemaName);

    @Override
    public void createRootDocPartTable(DSLContext dsl, String schemaName, String tableName, TableRef tableRef) {
        String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface.getInternalFields(tableRef),
                metaDataReadInterface.getPrimaryKeyInternalFields(tableRef));
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    @Override
    public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, TableRef tableRef, String foreignTableName) {
        String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface.getInternalFields(tableRef),
                metaDataReadInterface.getPrimaryKeyInternalFields(tableRef),
                metaDataReadInterface.getReferenceInternalFields(tableRef), foreignTableName, metaDataReadInterface.getForeignInternalFields(tableRef));
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
        String readIndexStatement = getCreateDocPartTableIndexStatement(schemaName, tableName, metaDataReadInterface.getReadInternalFields(tableRef));
        sqlHelper.executeStatement(dsl, readIndexStatement, Context.CREATE_INDEX);
    }

    protected abstract String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<InternalField<?>> fields, Collection<InternalField<?>> primaryKeyFields);

    protected abstract String getCreateDocPartTableIndexStatement(String schemaName, String tableName,
            Collection<InternalField<?>> indexedFields);

    protected abstract String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<InternalField<?>> fields, Collection<InternalField<?>> primaryKeyFields, 
            Collection<InternalField<?>> referenceFields, String foreignTableName, Collection<InternalField<?>> foreignFields);
    
    @Override
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, String columnName, DataTypeForKV<?> dataType) {
        String statement = getAddColumnToDocPartTableStatement(schemaName, tableName, columnName, dataType);
        
        sqlHelper.executeStatement(dsl, statement, Context.ADD_COLUMN);
    }

    protected abstract String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
            String columnName, DataTypeForKV<?> dataType);
}
