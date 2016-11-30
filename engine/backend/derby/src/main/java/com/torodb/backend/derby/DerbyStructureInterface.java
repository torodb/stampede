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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.torodb.backend.AbstractStructureInterface;
import com.torodb.backend.InternalField;
import com.torodb.backend.SqlBuilder;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.core.backend.IdentifierConstraints;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class DerbyStructureInterface extends AbstractStructureInterface {

  @Inject
  public DerbyStructureInterface(DerbyDbBackend dbBackend,
      DerbyMetaDataReadInterface metaDataReadInterface,
      SqlHelper sqlHelper,
      IdentifierConstraints identifierConstraints) {
    super(dbBackend, metaDataReadInterface, sqlHelper, identifierConstraints);
  }

  @Override
  protected String getDropTableStatement(String schemaName, String tableName) {
    return "DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"";
  }

  @Override
  protected String getRenameTableStatement(String fromSchemaName, String fromTableName,
      String toTableName) {
    return "RENAME TABLE \"" + fromSchemaName + "\".\"" + fromTableName + "\" TO  \"" + toTableName
        + "\"";
  }

  @Override
  protected String getRenameIndexStatement(String fromSchemaName, String fromIndexName,
      String toIndexName) {
    return "SET SCHEMA = \"" + fromSchemaName + "\"; RENAME INDEX \"" + fromIndexName + "\" TO \""
        + toIndexName + "\"";
  }

  @Override
  protected String getSetTableSchemaStatement(String fromSchemaName, String fromTableName,
      String toSchemaName) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String getDropSchemaStatement(String schemaName) {
    return "DROP SCHEMA \"" + schemaName + "\" RESTRICT";
  }

  @Override
  protected String getCreateIndexStatement(String indexName, String schemaName, String tableName,
      List<Tuple2<String, Boolean>> columnList, boolean unique) {
    StringBuilder sb = new StringBuilder()
        .append(unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
        .append("\"").append(indexName).append("\"")
        .append(" ON ")
        .append("\"").append(schemaName).append("\"")
        .append(".")
        .append("\"").append(tableName).append("\"")
        .append(" (");
    for (Tuple2<String, Boolean> columnEntry : columnList) {
      sb.append("\"").append(columnEntry.v1()).append("\" ")
          .append(columnEntry.v2() ? "ASC," : "DESC,");
    }
    sb.setCharAt(sb.length() - 1, ')');
    String statement = sb.toString();
    return statement;
  }

  @Override
  protected String getDropIndexStatement(String schemaName, String indexName) {
    StringBuilder sb = new StringBuilder()
        .append("DROP INDEX \"")
        .append(schemaName)
        .append("\".\"")
        .append(indexName)
        .append('"');
    return sb.toString();
  }

  @Override
  protected String getCreateSchemaStatement(String schemaName) {
    return "CREATE SCHEMA \"" + schemaName + "\"";
  }

  @Override
  protected String getCreateDocPartTableStatement(String schemaName, String tableName,
      Collection<InternalField<?>> fields) {
    SqlBuilder sb = new SqlBuilder("CREATE TABLE ");
    sb.table(schemaName, tableName)
        .append(" (");
    if (!fields.isEmpty()) {
      for (InternalField<?> field : fields) {
        sb.quote(field.getName()).append(' ')
            .append(field.getDataType().getCastTypeName());
        if (!field.isNullable()) {
          sb.append(" NOT NULL");
        }
        sb.append(',');
      }
      sb.setLastChar(')');
    } else {
      sb.append(')');
    }
    return sb.toString();
  }

  @Override
  protected String getAddDocPartTablePrimaryKeyStatement(String schemaName, String tableName,
      Collection<InternalField<?>> primaryKeyFields) {
    SqlBuilder sb = new SqlBuilder("ALTER TABLE ");
    sb.table(schemaName, tableName)
        .append(" ADD PRIMARY KEY (");
    for (InternalField<?> field : primaryKeyFields) {
      sb.quote(field.getName()).append(',');
    }
    sb.setLastChar(')');
    return sb.toString();
  }

  @Override
  protected String getAddDocPartTableForeignKeyStatement(String schemaName, String tableName,
      Collection<InternalField<?>> referenceFields, String foreignTableName,
      Collection<InternalField<?>> foreignFields) {
    Preconditions.checkArgument(referenceFields.size() == foreignFields.size());

    SqlBuilder sb = new SqlBuilder("ALTER TABLE ");
    sb.table(schemaName, tableName)
        .append(" ADD FOREIGN KEY (");
    for (InternalField<?> field : referenceFields) {
      sb.quote(field.getName()).append(',');
    }
    sb.setLastChar(')')
        .append(" REFERENCES ")
        .table(schemaName, foreignTableName)
        .append(" (");
    for (InternalField<?> field : foreignFields) {
      sb.quote(field.getName()).append(',');
    }
    sb.setLastChar(')');
    return sb.toString();
  }

  @Override
  protected String getCreateDocPartTableIndexStatement(String schemaName, String tableName,
      Collection<InternalField<?>> indexFields) {
    Preconditions.checkArgument(!indexFields.isEmpty());
    SqlBuilder sb = new SqlBuilder("CREATE INDEX ");

    String fieldPartName = Joiner.on("")
        .join(indexFields.stream()
            .map(field -> field.getName()).iterator()
        );

    sb.quote(tableName + fieldPartName + "_idx");
    sb.append(" ON ");
    sb.table(schemaName, tableName)
        .append(" (");
    for (InternalField<?> field : indexFields) {
      sb.quote(field.getName()).append(',');
    }
    sb.setLastChar(')');
    return sb.toString();
  }

  @Override
  protected String getAddColumnToDocPartTableStatement(String schemaName, String tableName,
      String columnName,
      DataTypeForKv<?> dataType) {
    SqlBuilder sb = new SqlBuilder("ALTER TABLE ")
        .table(schemaName, tableName)
        .append(" ADD COLUMN ")
        .quote(columnName)
        .append(" ")
        .append(dataType.getCastTypeName());
    return sb.toString();
  }

  @Override
  protected void dropDatabase(DSLContext dsl, String dbIdentifier) {
    //TODO: The general implementation is not implemented yet, as it
    //requires several reads to the metadata
    throw new UnsupportedOperationException("It is not defined yet how to "
        + "drop all user databases on a general way");
  }
}
