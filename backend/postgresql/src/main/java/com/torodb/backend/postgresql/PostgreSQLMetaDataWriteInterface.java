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

package com.torodb.backend.postgresql;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.torodb.backend.AbstractWriteMetaDataInterface;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaCollectionTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDatabaseTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaDocPartTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaFieldTable;
import com.torodb.backend.postgresql.tables.PostgreSQLMetaScalarTable;
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
public class PostgreSQLMetaDataWriteInterface extends AbstractWriteMetaDataInterface {

    private final PostgreSQLMetaDatabaseTable metaDatabaseTable;
    private final PostgreSQLMetaCollectionTable metaCollectionTable;
    private final PostgreSQLMetaDocPartTable metaDocPartTable;
    private final PostgreSQLMetaFieldTable metaFieldTable;
    private final PostgreSQLMetaScalarTable metaScalarTable;

    @Inject
    public PostgreSQLMetaDataWriteInterface(PostgreSQLMetaDataReadInterface postgreSQLMetaDataReadInterface, 
            SqlHelper sqlHelper) {
        super(postgreSQLMetaDataReadInterface, sqlHelper);
        this.metaDatabaseTable = postgreSQLMetaDataReadInterface.getMetaDatabaseTable();
        this.metaCollectionTable = postgreSQLMetaDataReadInterface.getMetaCollectionTable();
        this.metaDocPartTable = postgreSQLMetaDataReadInterface.getMetaDocPartTable();
        this.metaFieldTable = postgreSQLMetaDataReadInterface.getMetaFieldTable();
        this.metaScalarTable = postgreSQLMetaDataReadInterface.getMetaScalarTable();
    }

    @Override
    protected String getCreateMetaDatabaseTableStatement(String schemaName, String tableName) {
        String statement = new StringBuilder()
                .append("CREATE TABLE \"")
                .append(schemaName)
                .append("\".\"")
                .append(tableName)
                .append("\" (")
                .append('"').append(MetaDatabaseTable.TableFields.NAME.toString()).append('"').append("             varchar           PRIMARY KEY     ,")
                .append('"').append(MetaDatabaseTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar           NOT NULL UNIQUE ")
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
                .append('"').append(MetaCollectionTable.TableFields.DATABASE.toString()).append('"').append("         varchar           NOT NULL        ,")
                .append('"').append(MetaCollectionTable.TableFields.NAME.toString()).append('"').append("             varchar           NOT NULL        ,")
                .append('"').append(MetaDatabaseTable.TableFields.IDENTIFIER.toString()).append('"').append("         varchar           NOT NULL UNIQUE ,")
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
                .append('"').append(MetaDocPartTable.TableFields.DATABASE.toString()).append('"').append("         varchar           NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.COLLECTION.toString()).append('"').append("       varchar           NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar[]         NOT NULL        ,")
                .append('"').append(MetaDocPartTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar           NOT NULL        ,")
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
                .append('"').append(MetaFieldTable.TableFields.DATABASE.toString()).append('"').append("         varchar          NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.COLLECTION.toString()).append('"').append("       varchar          NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar[]        NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.NAME.toString()).append('"').append("             varchar          NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.TYPE.toString()).append('"').append("             varchar          NOT NULL        ,")
                .append('"').append(MetaFieldTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar          NOT NULL        ,")
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
                .append('"').append(MetaScalarTable.TableFields.DATABASE.toString()).append('"').append("         varchar          NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.COLLECTION.toString()).append('"').append("       varchar          NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.TABLE_REF.toString()).append('"').append("        varchar[]        NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.TYPE.toString()).append('"').append("             varchar          NOT NULL        ,")
                .append('"').append(MetaScalarTable.TableFields.IDENTIFIER.toString()).append('"').append("       varchar          NOT NULL        ,")
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
                .append('"').append(indexNameColumn).append('"').append("       varchar           PRIMARY KEY,")
                .append('"').append(indexOptionsColumn).append('"').append("    jsonb             NOT NULL")
                .append(")")
                .toString();
    }

	@Override
    protected String getAddMetaDatabaseStatement(String databaseName, String databaseIdentifier) {
        String statement = DSL.insertInto(metaDatabaseTable)
            .set(metaDatabaseTable.newRecord().values(databaseName, databaseIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

	@Override
    protected String getAddMetaCollectionStatement(String databaseName, String collectionName,
            String collectionIdentifier) {
        String statement = DSL.insertInto(metaCollectionTable)
            .set(metaCollectionTable.newRecord()
            .values(databaseName, collectionName, collectionIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }

	@Override
    protected String getAddMetaDocPartStatement(String databaseName, String collectionName, TableRef tableRef,
            String docPartIdentifier) {
        String statement = DSL.insertInto(metaDocPartTable)
            .set(metaDocPartTable.newRecord()
            .values(databaseName, collectionName, tableRef, docPartIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
	
	@Override
    protected String getAddMetaFieldStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldName, String fieldIdentifier, FieldType type) {
        String statement = DSL.insertInto(metaFieldTable)
                .set(metaFieldTable.newRecord()
                .values(databaseName, collectionName, tableRef, fieldName, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
	
	@Override
    protected String getAddMetaScalarStatement(String databaseName, String collectionName, TableRef tableRef,
            String fieldIdentifier, FieldType type) {
        String statement = DSL.insertInto(metaScalarTable)
				.set(metaScalarTable.newRecord()
				.values(databaseName, collectionName, tableRef, type, fieldIdentifier)).getSQL(ParamType.INLINED);
        return statement;
    }
    
    @Override
    public int consumeRids(DSLContext dsl, String database, String collection, TableRef tableRef, int count) {
        Record1<Integer> lastRid = dsl.select(metaDocPartTable.LAST_RID).from(metaDocPartTable).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toStringArray(tableRef))))
            .fetchOne();
        dsl.update(metaDocPartTable).set(metaDocPartTable.LAST_RID, metaDocPartTable.LAST_RID.plus(count)).where(
                metaDocPartTable.DATABASE.eq(database)
                .and(metaDocPartTable.COLLECTION.eq(collection))
                .and(metaDocPartTable.TABLE_REF.eq(TableRefConverter.toStringArray(tableRef)))).execute();
        return lastRid.value1();
    }
}
