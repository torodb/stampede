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

package com.torodb.backend.postgresql;

import com.torodb.backend.DbBackendService;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.backend.tests.common.DatabaseTestContext;
import com.torodb.core.backend.IdentifierConstraints;
import org.jooq.DSLContext;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgreSqlDatabaseTestContext implements DatabaseTestContext {

  private SqlInterface sqlInterface;

  private DslContextFactory dslContextFactory;

  private SchemaUpdater schemaUpdater;

  public PostgreSqlDatabaseTestContext(SqlInterface sqlInterface, DslContextFactory dslContextFactory,
                                  SchemaUpdater schemaUpdater) {
    this.sqlInterface = sqlInterface;
    this.dslContextFactory = dslContextFactory;
    this.schemaUpdater = schemaUpdater;
  }

  public void setupDatabase() throws SQLException {
    try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
      dropDatabase();

      DSLContext dslContext = dslContextFactory.createDslContext(connection);
      schemaUpdater.checkOrCreate(dslContext);

      connection.commit();
    }
  }

  public void tearDownDatabase() {
    sqlInterface.getDbBackend().stopAsync();
    sqlInterface.getDbBackend().awaitTerminated();
  }

  public SqlInterface getSqlInterface() {
    return sqlInterface;
  }

  @Override
  public DslContextFactory getDslContextFactory() {
    return dslContextFactory;
  }

  private void dropDatabase() throws SQLException {
    DbBackendService dbBackend = sqlInterface.getDbBackend();
    IdentifierConstraints identifierConstraints = sqlInterface.getIdentifierConstraints();
    try (Connection connection = dbBackend.createSystemConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet tables = metaData.getTables("%", "%", "%", new String[] { "TABLE", "VIEW" });
      List<String[]> nextToDropSchemaTableList = new ArrayList<>();

      while (tables.next()) {
        String schemaName = tables.getString("TABLE_SCHEM");
        String tableName = tables.getString("TABLE_NAME");
        nextToDropSchemaTableList.add(new String[] { schemaName, tableName });
      }

      while (!nextToDropSchemaTableList.isEmpty()) {
        List<String[]> toDropSchemaTableList = new ArrayList<>(nextToDropSchemaTableList);
        nextToDropSchemaTableList.clear();
        for (String[] toDropSchameTable : toDropSchemaTableList) {
          String schemaName = toDropSchameTable[0];
          String tableName = toDropSchameTable[1];
          if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE \"" + schemaName + "\".\"" + tableName + "\"")) {
              preparedStatement.executeUpdate();
              connection.commit();
            } catch(SQLException sqlException) {
              connection.rollback();
              nextToDropSchemaTableList.add(new String[] { schemaName, tableName });
            }
          }
        }
      }

      ResultSet schemas = metaData.getSchemas();
      while (schemas.next()) {
        String schemaName = schemas.getString("TABLE_SCHEM");
        if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) || schemaName.equals(TorodbSchema.IDENTIFIER)) {
          String dropSchemaStatement = "DROP SCHEMA \"" + schemaName + "\" CASCADE";
          try (PreparedStatement preparedStatement = connection.prepareStatement(dropSchemaStatement)) {
            preparedStatement.executeUpdate();
          }
        }
      }

      connection.commit();
    }
  }

}
