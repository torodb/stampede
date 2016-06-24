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

package com.torodb.backend.derby;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.conf.ParamType;

import com.torodb.backend.AbstractMetaDataWriteInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.derby.tables.DerbyMetaCollectionTable;
import com.torodb.backend.derby.tables.DerbyMetaDatabaseTable;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.derby.tables.DerbyMetaScalarTable;
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
public class DerbyMetaDataWriteInterface extends AbstractMetaDataWriteInterface {

    private final DerbyMetaDatabaseTable metaDatabaseTable;
    private final DerbyMetaCollectionTable metaCollectionTable;
    private final DerbyMetaDocPartTable metaDocPartTable;
    private final DerbyMetaFieldTable metaFieldTable;
    private final DerbyMetaScalarTable metaScalarTable;
    private final SqlHelper sqlHelper;

    @Inject
    public DerbyMetaDataWriteInterface(DerbyMetaDataReadInterface metaDataReadInterface, 
            SqlHelper sqlHelper) {
        super(metaDataReadInterface, sqlHelper);
        this.metaDatabaseTable = metaDataReadInterface.getMetaDatabaseTable();
        this.metaCollectionTable = metaDataReadInterface.getMetaCollectionTable();
        this.metaDocPartTable = metaDataReadInterface.getMetaDocPartTable();
        this.metaFieldTable = metaDataReadInterface.getMetaFieldTable();
        this.metaScalarTable = metaDataReadInterface.getMetaScalarTable();
        this.sqlHelper = sqlHelper;
    }

    @Override
    protected String getCreateMetaDatabaseTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaDatabaseTable.TableFields.NAME.toString()).append('"').append("             varchar(32672)    PRIMARY KEY     ,")
                .append('"').append(MetaDatabaseTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)      NOT NULL UNIQUE ")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaCollectionTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaCollectionTable.TableFields.DATABASE.toString()).append('"').append("         varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaCollectionTable.TableFields.NAME.toString()).append('"').append("             varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaDatabaseTable.TableFields.IDENTIFIER.toString()).append('"').append("         varchar(128)      NOT NULL UNIQUE ,")
                .append("    PRIMARY KEY (").append('"').append(MetaCollectionTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaCollectionTable.TableFields.NAME.toString()).append('"').append(")")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaDocPartTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append("         varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.COLLECTION.toString()).append('"').append("       varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar(32672)    NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)      NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.LAST_RID.toString()).append('"').append("         integer           NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.COLLECTION.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.TABLE_REF.toString()).append('"').append("),")
                .append("    UNIQUE (").append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaDocPartTable.TableFields.IDENTIFIER.toString()).append('"').append(")")
                .append(")")
                .toString();
        return statement;
    }

    @Override
    protected String getCreateMetaFieldTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append("         varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append("       varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.NAME.toString()).append('"').append("             varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TYPE.toString()).append('"').append("             varchar(128)     NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)     NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.NAME.toString()).append('"').append(",")
                .append('"').append(MetaFieldTable.TableFields.TYPE.toString()).append('"').append("),")
                .append("    UNIQUE (").append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                    .append('"').append(MetaFieldTable.TableFields.IDENTIFIER.toString()).append('"').append(")")
                .append(")")
                .toString();
        return statement;
    }

    @Override
    protected String getCreateMetaScalarTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaScalarTable.TableFields.DATABASE.toString()).append('"').append("         varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.COLLECTION.toString()).append('"').append("       varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar(32672)   NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.TYPE.toString()).append('"').append("             varchar(128)     NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar(128)     NOT NULL        ,")
                .append("    PRIMARY KEY (").append('"').append(MetaScalarTable.TableFields.DATABASE.toString()).append('"').append(",")
                .append('"').append(MetaScalarTable.TableFields.COLLECTION.toString()).append('"').append(",")
                .append('"').append(MetaScalarTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                .append('"').append(MetaScalarTable.TableFields.TYPE.toString()).append('"').append("),")
                .append("    UNIQUE (").append('"').append(MetaScalarTable.TableFields.DATABASE.toString()).append('"').append(",")
                    .append('"').append(MetaScalarTable.TableFields.COLLECTION.toString()).append('"').append(",")
                    .append('"').append(MetaScalarTable.TableFields.TABLE_REF.toString()).append('"').append(",")
                    .append('"').append(MetaScalarTable.TableFields.IDENTIFIER.toString()).append('"').append(")")
                .append(")")
                .toString();
        return statement;
    }

    @Override
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
    protected String getAddMetaDatabaseStatement(String databaseName, String databaseIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaDatabaseTable)
            .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

	@Override
    protected String getAddMetaCollectionStatement(String databaseName, String collectionName,
            String collectionIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaCollectionTable)
            .set(metaCollectionTable.newRecord()
            .values(databaseName, collectionName, collectionIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

	@Override
    protected String getAddMetaDocPartStatement(String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier) {
        String statement = sqlHelper.dsl().insertInto(metaDocPartTable)
            .set(metaDocPartTable.newRecord()
            .values(databaseName, collectionName, tableRef, docPartIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
	
	@Override
    protected String getAddMetaFieldStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldName, String fieldIdentifier, FieldType type) {
        String statement = sqlHelper.dsl().insertInto(metaFieldTable)
                .set(metaFieldTable.newRecord()
                .values(databaseName, collectionName, tableRef, fieldName, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
	
	@Override
    protected String getAddMetaScalarStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type) {
        String statement = sqlHelper.dsl().insertInto(metaScalarTable)
				.set(metaScalarTable.newRecord()
				.values(databaseName, collectionName, tableRef, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
    
    @Override
    public int consumeRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        Record1<Integer> lastRid = dsl.select(metaDocPartTable.LAST_RID).from(metaDocPartTable).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toJsonArray(tableRef))))
            .fetchOne();
        dsl.update(metaDocPartTable).set(metaDocPartTable.LAST_RID, metaDocPartTable.LAST_RID.plus(count)).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toJsonArray(tableRef)))).execute();
        return lastRid.value1();
    }
}
