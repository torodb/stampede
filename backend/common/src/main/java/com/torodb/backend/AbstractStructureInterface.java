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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;

/**
 *
 */
@Singleton
public abstract class AbstractStructureInterface implements StructureInterface {

    private final DbBackend dbBackend;
    private final MetaDataReadInterface metaDataReadInterface;
    private final SqlHelper sqlHelper;
    
    @Inject
    public AbstractStructureInterface(DbBackend dbBackend, MetaDataReadInterface metaDataReadInterface, SqlHelper sqlHelper) {
        this.dbBackend = dbBackend;
        this.metaDataReadInterface = metaDataReadInterface;
        this.sqlHelper = sqlHelper;
    }

    @Override
    public void dropDatabase(DSLContext dsl, MetaDatabase metaDatabase) {
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
    public void dropCollection(DSLContext dsl, String schemaName, MetaCollection metaCollection) {
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
    public void renameCollection(DSLContext dsl, String fromSchemaName, MetaCollection fromCollection, 
            String toSchemaName, MetaCollection toCollection) {
        Iterator<? extends MetaDocPart> metaDocPartIterator = fromCollection.streamContainedMetaDocParts().iterator();
        while (metaDocPartIterator.hasNext()) {
            MetaDocPart fromMetaDocPart = metaDocPartIterator.next();
            MetaDocPart toMetaDocPart = toCollection.getMetaDocPartByTableRef(fromMetaDocPart.getTableRef());
            String renameStatement = getRenameTableStatement(fromSchemaName, fromMetaDocPart.getIdentifier(), toMetaDocPart.getIdentifier());
            sqlHelper.executeUpdate(dsl, renameStatement, Context.RENAME_TABLE);
            
            if (!fromSchemaName.equals(toSchemaName)) {
                String setSchemaStatement = getSetTableSchemaStatement(fromSchemaName, fromMetaDocPart.getIdentifier(), toSchemaName);
                sqlHelper.executeUpdate(dsl, setSchemaStatement, Context.SET_TABLE_SCHEMA);
            }
        }
    }

    protected abstract String getRenameTableStatement(String fromSchemaName, String fromTableName, 
            String toTableName);

    protected abstract String getSetTableSchemaStatement(String fromSchemaName, String fromTableName, 
            String toSchemaName);
    
    @Override
    public void createIndex(DSLContext dsl, String indexName,
            String schemaName, String tableName,
            String columnName, boolean ascending, boolean unique
    ) {
        if (!dbBackend.isOnDataInsertMode()) {
            String statement = getCreateIndexStatement(indexName, schemaName, tableName, columnName, ascending, unique);

            sqlHelper.executeUpdate(dsl, statement, unique?Context.ADD_UNIQUE_INDEX:Context.CREATE_INDEX);
        }
    }

    protected abstract String getCreateIndexStatement(String indexName, String schemaName, String tableName, String columnName,
            boolean ascending, boolean unique);
    
    @Override
    public void dropIndex(DSLContext dsl, String schemaName, String indexName) {
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
    public void createSchema(DSLContext dsl, String schemaName){
    	String statement = getCreateSchemaStatement(schemaName);
    	sqlHelper.executeUpdate(dsl, statement, Context.CREATE_SCHEMA);
    }

    protected abstract String getCreateSchemaStatement(String schemaName);

    @Override
    public void createRootDocPartTable(DSLContext dsl, String schemaName, String tableName, TableRef tableRef) {
        String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface.getInternalFields(tableRef));
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    @Override
    public void createDocPartTable(DSLContext dsl, String schemaName, String tableName, TableRef tableRef, String foreignTableName) {
        String statement = getCreateDocPartTableStatement(schemaName, tableName, metaDataReadInterface.getInternalFields(tableRef));
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateDocPartTableStatement(String schemaName, String tableName,
            Collection<InternalField<?>> fields);

    @Override
    public Stream<Consumer<DSLContext>> streamRootDocPartTableIndexesCreation(String schemaName, String tableName, TableRef tableRef) {
        List<Consumer<DSLContext>> result = new ArrayList<>(1);
        if (!dbBackend.isOnDataInsertMode()) {
            String primaryKeyStatement = getAddDocPartTablePrimaryKeyStatement(schemaName, tableName, metaDataReadInterface.getPrimaryKeyInternalFields(tableRef));

            result.add(dsl -> sqlHelper.executeStatement(dsl, primaryKeyStatement, Context.ADD_UNIQUE_INDEX));
        }
        return result.stream();
    }

    @Override
    public Stream<Consumer<DSLContext>> streamDocPartTableIndexesCreation(String schemaName, String tableName, TableRef tableRef, String foreignTableName) {
        List<Consumer<DSLContext>> result = new ArrayList<>(4);
        if (!dbBackend.isOnDataInsertMode()) {
            String primaryKeyStatement = getAddDocPartTablePrimaryKeyStatement(schemaName, tableName, metaDataReadInterface.getPrimaryKeyInternalFields(tableRef));
            result.add( (dsl) -> sqlHelper.executeStatement(dsl, primaryKeyStatement, Context.ADD_UNIQUE_INDEX));
        }

        if (!dbBackend.isOnDataInsertMode()) {
            if (dbBackend.includeForeignKeys()) {
                String foreignKeyStatement = getAddDocPartTableForeignKeyStatement(schemaName, tableName, metaDataReadInterface.getReferenceInternalFields(tableRef),
                        foreignTableName, metaDataReadInterface.getForeignInternalFields(tableRef));
                result.add( (dsl) -> sqlHelper.executeStatement(dsl, foreignKeyStatement, Context.ADD_FOREIGN_KEY));
            } else {
                String foreignKeyIndexStatement = getCreateDocPartTableIndexStatement(schemaName, tableName, metaDataReadInterface.getReferenceInternalFields(tableRef));
                result.add( (dsl) -> sqlHelper.executeStatement(dsl, foreignKeyIndexStatement, Context.CREATE_INDEX));
            }
        }

        if (!dbBackend.isOnDataInsertMode()) {
            String readIndexStatement = getCreateDocPartTableIndexStatement(schemaName, tableName, metaDataReadInterface.getReadInternalFields(tableRef));
            result.add( (dsl) -> sqlHelper.executeStatement(dsl, readIndexStatement, Context.CREATE_INDEX));
        }

        return result.stream();
    }

    @Override
    public Stream<Consumer<DSLContext>> streamDataInsertFinishTasks(MetaSnapshot snapshot) {
        return Collections.<Consumer<DSLContext>>emptySet().stream();
    }

    protected abstract String getAddDocPartTablePrimaryKeyStatement(String schemaName, String tableName,
            Collection<InternalField<?>> primaryKeyFields);

    protected abstract String getAddDocPartTableForeignKeyStatement(String schemaName, String tableName,
            Collection<InternalField<?>> referenceFields, String foreignTableName, Collection<InternalField<?>> foreignFields);

    protected abstract String getCreateDocPartTableIndexStatement(String schemaName, String tableName,
            Collection<InternalField<?>> indexedFields);
    
    @Override
    public void addColumnToDocPartTable(DSLContext dsl, String schemaName, String tableName, String columnName, DataTypeForKV<?> dataType) {
        String statement = getAddColumnToDocPartTableStatement(schemaName, tableName, columnName, dataType);
        
        sqlHelper.executeStatement(dsl, statement, Context.ADD_COLUMN);
    }

    protected abstract String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
            String columnName, DataTypeForKV<?> dataType);
}
