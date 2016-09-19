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

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.TableField;

import com.torodb.backend.AbstractMetaDataWriteInterface;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.derby.tables.DerbyMetaDocPartTable;
import com.torodb.backend.derby.tables.DerbyMetaFieldTable;
import com.torodb.backend.derby.tables.DerbyMetaScalarTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldIndexTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;

/**
 *
 */
@Singleton
public class DerbyMetaDataWriteInterface extends AbstractMetaDataWriteInterface {

    private final DerbyMetaDocPartTable metaDocPartTable;
    private final DerbyMetaFieldTable metaFieldTable;
    private final DerbyMetaScalarTable metaScalarTable;

    @Inject
    public DerbyMetaDataWriteInterface(DerbyMetaDataReadInterface metaDataReadInterface,
            DerbyMetaFieldTable metaFieldTable,
            DerbyMetaScalarTable metaScalarTable,
            SqlHelper sqlHelper) {
        super(metaDataReadInterface, sqlHelper);
        this.metaDocPartTable = metaDataReadInterface.getMetaDocPartTable();
        this.metaFieldTable = metaDataReadInterface.getMetaFieldTable();
        this.metaScalarTable = metaDataReadInterface.getMetaScalarTable();
    }

    @Override
    protected String getCreateMetaDatabaseTableStatement(String schemaName, String tableName) {
    	String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
    	    	.append(" (")
    	    	.quote(MetaDatabaseTable.TableFields.NAME).append("       varchar(32672)    PRIMARY KEY     ,")
    	    	.quote(MetaDatabaseTable.TableFields.IDENTIFIER).append(" varchar(128)      NOT NULL UNIQUE ")
    	        .append(")")
    	        .toString();
    	return statement;
    }
    
    @Override
    protected String getCreateMetaCollectionTableStatement(String schemaName, String tableName) {
    	String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
    	    	.append(" (")
                .quote(MetaCollectionTable.TableFields.DATABASE).append(" varchar(32672)    NOT NULL        ,")
                .quote(MetaCollectionTable.TableFields.NAME).append("     varchar(32672)    NOT NULL        ,")
                .quote(MetaDatabaseTable.TableFields.IDENTIFIER).append(" varchar(128)      NOT NULL UNIQUE ,")
                	.append("    PRIMARY KEY (").quote(MetaCollectionTable.TableFields.DATABASE).append(",")
                .quote(MetaCollectionTable.TableFields.NAME).append(")")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaDocPartTableStatement(String schemaName, String tableName) {
    	String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
    	    	.append(" (")
    	    	.quote(MetaDocPartTable.TableFields.DATABASE).append("   varchar(32672)    NOT NULL        ,")
    	    	.quote(MetaDocPartTable.TableFields.COLLECTION).append(" varchar(32672)    NOT NULL        ,")
    	    	.quote(MetaDocPartTable.TableFields.TABLE_REF).append("  varchar(32672)    NOT NULL        ,")
    	    	.quote(MetaDocPartTable.TableFields.IDENTIFIER).append(" varchar(128)      NOT NULL        ,")
    	    	.quote(MetaDocPartTable.TableFields.LAST_RID).append("   integer           NOT NULL        ,")
                .append("    PRIMARY KEY (").quote(MetaDocPartTable.TableFields.DATABASE).append(",")
                .quote(MetaDocPartTable.TableFields.COLLECTION).append(",")
                .quote(MetaDocPartTable.TableFields.TABLE_REF).append("),")
                .append("    UNIQUE (").quote(MetaDocPartTable.TableFields.DATABASE).append(",")
                .quote(MetaDocPartTable.TableFields.IDENTIFIER).append(")")
                .append(")")
                .toString();
        return statement;
    }

    @Override
    protected String getCreateMetaFieldTableStatement(String schemaName, String tableName) {
    	String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
    	    	.append(" (")
    	    	.quote(MetaFieldTable.TableFields.DATABASE).append("   varchar(32672)   NOT NULL        ,")
    	    	.quote(MetaFieldTable.TableFields.COLLECTION).append(" varchar(32672)   NOT NULL        ,")
    	    	.quote(MetaFieldTable.TableFields.TABLE_REF).append("  varchar(32672)   NOT NULL        ,")
    	    	.quote(MetaFieldTable.TableFields.NAME).append("       varchar(32672)   NOT NULL        ,")
    	    	.quote(MetaFieldTable.TableFields.TYPE).append("       varchar(128)     NOT NULL        ,")
    	    	.quote(MetaFieldTable.TableFields.IDENTIFIER).append(" varchar(128)     NOT NULL        ,")
                .append("    PRIMARY KEY (").quote(MetaFieldTable.TableFields.DATABASE).append(",")
                .quote(MetaFieldTable.TableFields.COLLECTION).append(",")
                .quote(MetaFieldTable.TableFields.TABLE_REF).append(",")
                .quote(MetaFieldTable.TableFields.NAME).append(",")
                .quote(MetaFieldTable.TableFields.TYPE).append("),")
                .append("    UNIQUE (").quote(MetaFieldTable.TableFields.DATABASE).append(",")
                .quote(MetaFieldTable.TableFields.COLLECTION).append(",")
                .quote(MetaFieldTable.TableFields.TABLE_REF).append(",")
                .quote(MetaFieldTable.TableFields.IDENTIFIER).append(")")
                .append(")")
                .toString();
        return statement;
    }

    @Override
    protected String getCreateMetaScalarTableStatement(String schemaName, String tableName) {
    	String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
    	    	.append(" (")
                .quote(MetaScalarTable.TableFields.DATABASE).append("   varchar(32672)   NOT NULL        ,")
                .quote(MetaScalarTable.TableFields.COLLECTION).append(" varchar(32672)   NOT NULL        ,")
                .quote(MetaScalarTable.TableFields.TABLE_REF).append("  varchar(32672)   NOT NULL        ,")
                .quote(MetaScalarTable.TableFields.TYPE).append("       varchar(128)     NOT NULL        ,")
                .quote(MetaScalarTable.TableFields.IDENTIFIER).append(" varchar(128)     NOT NULL        ,")
                .append("    PRIMARY KEY (").quote(MetaScalarTable.TableFields.DATABASE).append(",")
                .quote(MetaScalarTable.TableFields.COLLECTION).append(",")
                .quote(MetaScalarTable.TableFields.TABLE_REF).append(",")
                .quote(MetaScalarTable.TableFields.TYPE).append("),")
                .append("    UNIQUE (").quote(MetaScalarTable.TableFields.DATABASE).append(",")
                    .quote(MetaScalarTable.TableFields.COLLECTION).append(",")
                    .quote(MetaScalarTable.TableFields.TABLE_REF).append(",")
                    .quote(MetaScalarTable.TableFields.IDENTIFIER).append(")")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaIndexTableStatement(String schemaName, String tableName) {
        String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
                .append(" (")
                .quote(MetaIndexTable.TableFields.DATABASE).append(" varchar(32672)  NOT NULL        ,")
                .quote(MetaIndexTable.TableFields.COLLECTION).append(" varchar(32672)  NOT NULL        ,")
                .quote(MetaIndexTable.TableFields.NAME).append("     varchar(32672)  NOT NULL        ,")
                .append("    PRIMARY KEY (").quote(MetaIndexTable.TableFields.DATABASE).append(",")
                    .quote(MetaIndexTable.TableFields.COLLECTION).append(",")
                    .quote(MetaIndexTable.TableFields.NAME).append(")")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaIndexFieldTableStatement(String schemaName, String tableName) {
        String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
                .append(" (")
                .quote(MetaIndexFieldTable.TableFields.DATABASE).append("   varchar(32672)   NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.COLLECTION).append(" varchar(32672)   NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.INDEX).append(" varchar(32672)   NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.POSITION).append(" integer   NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.TABLE_REF).append("  varchar(32672) NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.NAME).append(" varchar(32672)   NOT NULL ,")
                .quote(MetaIndexFieldTable.TableFields.ORDERING).append("   varchar(32672)   NOT NULL ,")
                .append("    PRIMARY KEY (").quote(MetaIndexFieldTable.TableFields.DATABASE).append(",")
                    .quote(MetaIndexFieldTable.TableFields.COLLECTION).append(",")
                    .quote(MetaIndexFieldTable.TableFields.INDEX).append(",")
                    .quote(MetaIndexFieldTable.TableFields.POSITION).append("),")
                .append("    UNIQUE (").quote(MetaIndexFieldTable.TableFields.DATABASE).append(",")
                    .quote(MetaIndexFieldTable.TableFields.COLLECTION).append(",")
                    .quote(MetaIndexFieldTable.TableFields.INDEX).append(",")
                    .quote(MetaIndexFieldTable.TableFields.TABLE_REF).append(",")
                    .quote(MetaIndexFieldTable.TableFields.NAME).append(")")
                .append(")")
                .toString();
        return statement;
    }
    
    @Override
    protected String getCreateMetaDocPartIndexTableStatement(String schemaName, String tableName) {
        String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
                .append(" (")
                .quote(MetaDocPartTable.TableFields.DATABASE).append("   varchar(32672)   NOT NULL ,")
                .quote(MetaDocPartTable.TableFields.IDENTIFIER).append(" varchar(128)   NOT NULL ,")
                .quote(MetaDocPartTable.TableFields.COLLECTION).append(" varchar(32672)   NOT NULL ,")
                .quote(MetaDocPartTable.TableFields.TABLE_REF).append("  varchar(32672) NOT NULL ,")
                .append("    PRIMARY KEY (").quote(MetaDocPartTable.TableFields.DATABASE).append(",")
                    .quote(MetaDocPartTable.TableFields.IDENTIFIER).append("),")
                .append(")")
                .toString();
        return statement;
    }

    @Override
    protected String getCreateMetaFieldIndexTableStatement(String schemaName, String tableName) {
        String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
                .append(" (")
                .quote(MetaFieldIndexTable.TableFields.DATABASE).append("   varchar(32672)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.IDENTIFIER).append(" varchar(128)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.POSITION).append(" varchar(32672)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.COLLECTION).append(" varchar(32672)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.TABLE_REF).append("  varchar(32672)   NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.NAME).append("       varchar(32672)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.TYPE).append("       varchar(128)     NOT NULL ,")
                .quote(MetaFieldIndexTable.TableFields.ORDERING).append("       varchar(128)     NOT NULL ,")
                .append("    PRIMARY KEY (").quote(MetaFieldIndexTable.TableFields.DATABASE).append(",")
                    .quote(MetaFieldIndexTable.TableFields.IDENTIFIER).append(",")
                    .quote(MetaFieldIndexTable.TableFields.POSITION).append("),")
                .append(")")
                .toString();
        return statement;
    }


    @SuppressWarnings("unchecked")
    @Override
    protected Condition getTableRefEqCondition(@SuppressWarnings("rawtypes") TableField field, TableRef tableRef) {
        return field.eq(TableRefConverter.toJsonArray(tableRef));
    }

}
