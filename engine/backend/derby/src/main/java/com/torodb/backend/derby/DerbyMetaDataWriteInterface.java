/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.derby;

import com.torodb.backend.AbstractMetaDataWriteInterface;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.TableRefConverter;
import com.torodb.backend.tables.KvTable;
import com.torodb.backend.tables.MetaCollectionTable;
import com.torodb.backend.tables.MetaDatabaseTable;
import com.torodb.backend.tables.MetaDocPartIndexColumnTable;
import com.torodb.backend.tables.MetaDocPartIndexTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.MetaFieldTable;
import com.torodb.backend.tables.MetaIndexFieldTable;
import com.torodb.backend.tables.MetaIndexTable;
import com.torodb.backend.tables.MetaScalarTable;
import com.torodb.core.TableRef;
import org.jooq.Condition;
import org.jooq.TableField;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DerbyMetaDataWriteInterface extends AbstractMetaDataWriteInterface {

  @Inject
  public DerbyMetaDataWriteInterface(DerbyMetaDataReadInterface metaDataReadInterface,
      SqlHelper sqlHelper) {
    super(metaDataReadInterface, sqlHelper);
  }

  @Override
  protected String getCreateMetaDatabaseTableStatement(String schemaName, String tableName) {
    String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
        .append(" (")
        .quote(MetaDatabaseTable.TableFields.NAME).append(
        "       varchar(32672)    PRIMARY KEY     ,")
        .quote(MetaDatabaseTable.TableFields.IDENTIFIER).append(
        " varchar(128)      NOT NULL UNIQUE ")
        .append(")")
        .toString();
    return statement;
  }

  @Override
  protected String getCreateMetaCollectionTableStatement(String schemaName, String tableName) {
    String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
        .append(" (")
        .quote(MetaCollectionTable.TableFields.DATABASE).append(
        " varchar(32672)    NOT NULL        ,")
        .quote(MetaCollectionTable.TableFields.NAME).append(
        "     varchar(32672)    NOT NULL        ,")
        .quote(MetaDatabaseTable.TableFields.IDENTIFIER).append(
        " varchar(128)      NOT NULL UNIQUE ,")
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
        .quote(MetaDocPartTable.TableFields.DATABASE).append(
        "   varchar(32672)    NOT NULL        ,")
        .quote(MetaDocPartTable.TableFields.COLLECTION).append(
        " varchar(32672)    NOT NULL        ,")
        .quote(MetaDocPartTable.TableFields.TABLE_REF).append(
        "  varchar(32672)    NOT NULL        ,")
        .quote(MetaDocPartTable.TableFields.IDENTIFIER).append(
        " varchar(128)      NOT NULL        ,")
        .quote(MetaDocPartTable.TableFields.LAST_RID).append(
        "   integer           NOT NULL        ,")
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
        .quote(MetaIndexTable.TableFields.UNIQUE).append("     boolean  NOT NULL        ,")
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
        .quote(MetaDocPartIndexTable.TableFields.DATABASE).append("   varchar(32672)   NOT NULL ,")
        .quote(MetaDocPartIndexTable.TableFields.IDENTIFIER).append(" varchar(128)   NOT NULL ,")
        .quote(MetaDocPartIndexTable.TableFields.COLLECTION).append(" varchar(32672)   NOT NULL ,")
        .quote(MetaDocPartIndexTable.TableFields.TABLE_REF).append("  varchar(32672) NOT NULL ,")
        .quote(MetaDocPartIndexTable.TableFields.UNIQUE).append("     boolean  NOT NULL        ,")
        .append("    PRIMARY KEY (").quote(MetaDocPartIndexTable.TableFields.DATABASE).append(",")
        .quote(MetaDocPartIndexTable.TableFields.IDENTIFIER).append(")")
        .append(")")
        .toString();
    return statement;
  }

  @Override
  protected String getCreateMetaDocPartIndexColumnTableStatement(String schemaName,
      String tableName) {
    String statement = new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
        .append(" (")
        .quote(MetaDocPartIndexColumnTable.TableFields.DATABASE).append(
        "   varchar(32672)     NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.INDEX_IDENTIFIER).append(
        " varchar(128)     NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.POSITION).append(
        " varchar(32672)     NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.COLLECTION).append(
        " varchar(32672)     NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.TABLE_REF).append(
        "  varchar(32672)   NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.IDENTIFIER).append(
        "       varchar(32672)     NOT NULL ,")
        .quote(MetaDocPartIndexColumnTable.TableFields.ORDERING).append(
        "       varchar(128)     NOT NULL ,")
        .append("    PRIMARY KEY (").quote(MetaDocPartIndexColumnTable.TableFields.DATABASE).append(
        ",")
        .quote(MetaDocPartIndexColumnTable.TableFields.INDEX_IDENTIFIER).append(",")
        .quote(MetaDocPartIndexColumnTable.TableFields.POSITION).append("),")
        .append("    UNIQUE (").quote(MetaDocPartIndexColumnTable.TableFields.DATABASE).append(",")
        .quote(MetaDocPartIndexColumnTable.TableFields.INDEX_IDENTIFIER).append(",")
        .quote(MetaDocPartIndexColumnTable.TableFields.IDENTIFIER).append(")")
        .append(")")
        .toString();
    return statement;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Condition getTableRefEqCondition(@SuppressWarnings("rawtypes") TableField field,
      TableRef tableRef) {
    return field.eq(TableRefConverter.toJsonArray(tableRef));
  }

  @Override
  protected String getCreateMetainfStatement(String schemaName, String tableName) {
    return new SqlBuilder("CREATE TABLE ").table(schemaName, tableName)
        .append(" (")
        .quote(KvTable.TableFields.KEY).append(" varchar(256) NOT NULL, ")
        .quote(KvTable.TableFields.VALUE).append(" varchar(32672) NOT NULL,")
        .append("  PRIMARY KEY (").quote(KvTable.TableFields.KEY).append(')')
        .append(')')
        .toString();
  }

}
