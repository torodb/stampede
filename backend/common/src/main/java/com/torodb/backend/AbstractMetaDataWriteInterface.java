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

import javax.annotation.Nonnull;
import javax.inject.Singleton;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.conf.ParamType;

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;

/**
 *
 */
@Singleton
public abstract class AbstractMetaDataWriteInterface implements MetaDataWriteInterface {

    private final MetaDatabaseTable<?> metaDatabaseTable;
    private final MetaCollectionTable<?> metaCollectionTable;
    private final MetaDocPartTable<?, ?> metaDocPartTable;
    private final MetaFieldTable<?, ?> metaFieldTable;
    private final MetaScalarTable<?, ?> metaScalarTable;
    private final SqlHelper sqlHelper;
    
    public AbstractMetaDataWriteInterface(MetaDataReadInterface derbyMetaDataReadInterface, 
            SqlHelper sqlHelper) {
        this.metaDatabaseTable = derbyMetaDataReadInterface.getMetaDatabaseTable();
        this.metaCollectionTable = derbyMetaDataReadInterface.getMetaCollectionTable();
        this.metaDocPartTable = derbyMetaDataReadInterface.getMetaDocPartTable();
        this.metaFieldTable = derbyMetaDataReadInterface.getMetaFieldTable();
        this.metaScalarTable = derbyMetaDataReadInterface.getMetaScalarTable();
        this.sqlHelper = sqlHelper;
    }

    @Override
    public void createMetaDatabaseTable(DSLContext dsl) {
    	String schemaName = metaDatabaseTable.getSchema().getName();
    	String tableName = metaDatabaseTable.getName();
        String statement = getCreateMetaDatabaseTableStatement(schemaName, tableName);
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateMetaDatabaseTableStatement(String schemaName, String tableName);
    
    @Override
    public void createMetaCollectionTable(DSLContext dsl) {
    	String schemaName = metaCollectionTable.getSchema().getName();
    	String tableName = metaCollectionTable.getName();
    	String statement = getCreateMetaCollectionTableStatement(schemaName, tableName);
    	sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateMetaCollectionTableStatement(String schemaName, String tableName);
    
    @Override
    public void createMetaDocPartTable(DSLContext dsl) {
    	String schemaName = metaDocPartTable.getSchema().getName();
    	String tableName = metaDocPartTable.getName();
    	String statement = getCreateMetaDocPartTableStatement(schemaName, tableName);
    	sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateMetaDocPartTableStatement(String schemaName, String tableName);

    @Override
    public void createMetaFieldTable(DSLContext dsl) {
    	String schemaName = metaFieldTable.getSchema().getName();
    	String tableName = metaFieldTable.getName();
    	String statement = getCreateMetaFieldTableStatement(schemaName, tableName);
    	sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateMetaFieldTableStatement(String schemaName, String tableName);
    
    @Override
    public void createMetaScalarTable(DSLContext dsl) {
        String schemaName = metaScalarTable.getSchema().getName();
        String tableName = metaScalarTable.getName();
        String statement = getCreateMetaScalarTableStatement(schemaName, tableName);
        sqlHelper.executeStatement(dsl, statement, Context.CREATE_TABLE);
    }

    protected abstract String getCreateMetaScalarTableStatement(String schemaName, String tableName);

    @Override
    public @Nonnull String createMetaIndexesTableStatement(
            @Nonnull String schemaName, @Nonnull String tableName, @Nonnull String indexNameColumn, @Nonnull String indexOptionsColumn
    ) {
        return getCreateMetaIndexesTableStatement(schemaName, tableName, indexNameColumn, indexOptionsColumn);
    }

    protected String getCreateMetaIndexesTableStatement(String schemaName, String tableName, String indexNameColumn,
            String indexOptionsColumn) {
        return new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(indexNameColumn).append('"').append("       varchar(32672)    PRIMARY KEY,")
                .append('"').append(indexOptionsColumn).append('"').append("    varchar(23672)    NOT NULL")
                .append(")")
                .toString();
    }

	@Override
	public void addMetaDatabase(DSLContext dsl, String databaseName, String databaseIdentifier) {
        String statement = getAddMetaDatabaseStatement(databaseName, databaseIdentifier);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    @Override
    public void addMetaCollection(DSLContext dsl, String databaseName, String collectionName, String collectionIdentifier) {
        String statement = getAddMetaCollectionStatement(databaseName, collectionName, collectionIdentifier);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
    }

    @Override
    public void addMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier) {
        String statement = getAddMetaDocPartStatement(databaseName, collectionName, tableRef, docPartIdentifier);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
    }

	@Override
	public void addMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldName, String fieldIdentifier, FieldType type) {
	    String statement = getAddMetaFieldStatement(databaseName, collectionName, tableRef, fieldName, fieldIdentifier,
                type);
		sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

	@Override
	public void addMetaScalar(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldIdentifier, FieldType type) {
	    String statement = getAddMetaScalarStatement(databaseName, collectionName, tableRef, fieldIdentifier, type);
		sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    protected String getAddMetaDatabaseStatement(String databaseName, String databaseIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaDatabaseTable)
            .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

    protected String getAddMetaCollectionStatement(String databaseName, String collectionName,
            String collectionIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaCollectionTable)
            .set(metaCollectionTable.newRecord()
            .values(databaseName, collectionName, collectionIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

    protected String getAddMetaDocPartStatement(String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaDocPartTable)
            .set(metaDocPartTable.newRecord()
            .values(databaseName, collectionName, tableRef, docPartIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
    
    protected String getAddMetaFieldStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldName, String fieldIdentifier, FieldType type) {
        String statement = sqlHelper.dsl().insertInto(metaFieldTable)
                .set(metaFieldTable.newRecord()
                .values(databaseName, collectionName, tableRef, fieldName, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
    
    protected String getAddMetaScalarStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type) {
        String statement = sqlHelper.dsl().insertInto(metaScalarTable)
                .set(metaScalarTable.newRecord()
                .values(databaseName, collectionName, tableRef, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

    @Override
    public void deleteMetaDatabase(DSLContext dsl, String databaseName) {
        String statement = getDeleteMetaDatabaseStatement(databaseName);
        sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    }

    @Override
    public void deleteMetaCollection(DSLContext dsl, String databaseName, String collectionName) {
        String statement = getDeleteMetaCollectionStatement(databaseName, collectionName);
        sqlHelper.executeUpdate(dsl, statement, Context.META_DELETE);
    }

    @Override
    public void deleteMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef) {
        String statement = getDeleteMetaDocPartStatement(databaseName, collectionName, tableRef);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
    }

    @Override
    public void deleteMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            String fieldName, FieldType type) {
        String statement = getDeleteMetaFieldStatement(databaseName, collectionName, tableRef, fieldName,
                type);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
    }

    @Override
    public void deleteMetaScalar(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
            FieldType type) {
        String statement = getDeleteMetaScalarStatement(databaseName, collectionName, tableRef, type);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
    }

    protected String getDeleteMetaDatabaseStatement(String databaseName) {
        String statement = sqlHelper.dsl().deleteFrom(metaDatabaseTable)
                .where(metaDatabaseTable.NAME.eq(databaseName)).getSQL(ParamType.INLINED);
            return statement;
    }

    protected String getDeleteMetaCollectionStatement(String databaseName, String collectionName) {
        String statement = sqlHelper.dsl().deleteFrom(metaCollectionTable)
                .where(metaCollectionTable.DATABASE.eq(databaseName)
                    .and(metaCollectionTable.NAME.eq(collectionName))).getSQL(ParamType.INLINED);
            return statement;
    }

    protected String getDeleteMetaDocPartStatement(String databaseName, String collectionName, TableRef tableRef) {
        String statement = sqlHelper.dsl().deleteFrom(metaDocPartTable)
            .where(metaDocPartTable.DATABASE.eq(databaseName)
                    .and(metaDocPartTable.COLLECTION.eq(collectionName))
                    .and(getMetaDocPartTableRefCondition(tableRef))).getSQL(ParamType.INLINED);
        return statement;
    }

    protected abstract Condition getMetaDocPartTableRefCondition(TableRef tableRef);
    
    protected String getDeleteMetaFieldStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldName, FieldType type) {
        String statement = sqlHelper.dsl().deleteFrom(metaFieldTable)
                .where(metaFieldTable.DATABASE.eq(databaseName)
                        .and(metaFieldTable.COLLECTION.eq(collectionName))
                        .and(getMetaFieldTableRefCondition(tableRef))
                        .and(metaFieldTable.NAME.eq(fieldName))
                        .and(metaFieldTable.TYPE.eq(type))).getSQL(ParamType.INLINED);
        return statement;
    }

    protected abstract Condition getMetaFieldTableRefCondition(TableRef tableRef);
    
    protected String getDeleteMetaScalarStatement(String databaseName, String collectionName, TableRef tableRef,
            FieldType type) {
        String statement = sqlHelper.dsl().deleteFrom(metaScalarTable)
                .where(metaScalarTable.DATABASE.eq(databaseName)
                        .and(metaScalarTable.COLLECTION.eq(collectionName))
                        .and(getMetaScalarTableRefCondition(tableRef))
                        .and(metaScalarTable.TYPE.eq(type))).getSQL(ParamType.INLINED);
        return statement;
    }
    
    protected abstract Condition getMetaScalarTableRefCondition(TableRef tableRef);
}
