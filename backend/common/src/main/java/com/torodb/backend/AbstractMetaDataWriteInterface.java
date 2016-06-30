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

import org.jooq.DSLContext;

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

    protected abstract String getAddMetaDatabaseStatement(String databaseName, String databaseIdentifier);

	@Override
	public void addMetaCollection(DSLContext dsl, String databaseName, String collectionName, String collectionIdentifier) {
	    String statement = getAddMetaCollectionStatement(databaseName, collectionName, collectionIdentifier);
        sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    protected abstract String getAddMetaCollectionStatement(String databaseName, String collectionName,
            String collectionIdentifier);

	@Override
	public void addMetaDocPart(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String docPartIdentifier) {
	    String statement = getAddMetaDocPartStatement(databaseName, collectionName, tableRef, docPartIdentifier);
		sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    protected abstract String getAddMetaDocPartStatement(String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier);
	
	@Override
	public void addMetaField(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldName, String fieldIdentifier, FieldType type) {
	    String statement = getAddMetaFieldStatement(databaseName, collectionName, tableRef, fieldName, fieldIdentifier,
                type);
		sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    protected abstract String getAddMetaFieldStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldName, String fieldIdentifier, FieldType type);
	
	@Override
	public void addMetaScalar(DSLContext dsl, String databaseName, String collectionName, TableRef tableRef,
			String fieldIdentifier, FieldType type) {
	    String statement = getAddMetaScalarStatement(databaseName, collectionName, tableRef, fieldIdentifier, type);
		sqlHelper.executeUpdate(dsl, statement, Context.META_INSERT);
	}

    protected abstract String getAddMetaScalarStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type);
}
