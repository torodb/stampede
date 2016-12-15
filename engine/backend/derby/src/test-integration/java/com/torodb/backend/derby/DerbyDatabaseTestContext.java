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

import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.meta.SchemaUpdater;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DerbyDatabaseTestContext implements DatabaseTextContext {

  private static final Logger LOGGER = LogManager.getLogger(DerbyDatabaseTestContext.class);

  private SqlInterface sqlInterface;

  private DslContextFactory dslContextFactory;

  private SchemaUpdater schemaUpdater;

  private DerbyDbBackendConfiguration configuration;

  public DerbyDatabaseTestContext(SqlInterface sqlInterface, DslContextFactory dslContextFactory,
                                  SchemaUpdater schemaUpdater, DerbyDbBackendConfiguration configuration) {
    this.sqlInterface = sqlInterface;
    this.dslContextFactory = dslContextFactory;
    this.schemaUpdater = schemaUpdater;
    this.configuration = configuration;
  }

  public void setupDatabase() throws SQLException {
    try (Connection connection = sqlInterface.getDbBackend().createWriteConnection()) {
      DSLContext dslContext = dslContextFactory.createDslContext(connection);
      schemaUpdater.checkOrCreate(dslContext);

      connection.commit();
    }
  }

  public void tearDownDatabase() {
    try {
      sqlInterface.getDbBackend().stopAsync();
      sqlInterface.getDbBackend().awaitTerminated();

      DriverManager.getConnection("jdbc:derby:memory:" + configuration.getDbName() + ";drop=true").close();
    } catch (SQLException e) {
      LOGGER.info("Database dropped", e);
    }
  }

  public SqlInterface getSqlInterface() {
    return sqlInterface;
  }

  @Override
  public DslContextFactory getDslContextFactory() {
    return dslContextFactory;
  }

}

