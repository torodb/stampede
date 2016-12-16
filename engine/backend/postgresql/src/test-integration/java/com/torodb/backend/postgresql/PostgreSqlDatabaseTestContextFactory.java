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

import com.torodb.backend.*;
import com.torodb.backend.common.DatabaseTestContext;
import com.torodb.backend.driver.postgresql.OfficialPostgreSqlDriver;
import com.torodb.backend.driver.postgresql.PostgreSqlBackendConfiguration;
import com.torodb.backend.driver.postgresql.PostgreSqlDriverProvider;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.postgresql.meta.PostgreSqlSchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class PostgreSqlDatabaseTestContextFactory {

  public DatabaseTestContext createInstance() {
    PostgreSqlErrorHandler errorHandler = new PostgreSqlErrorHandler();
    DataTypeProvider provider = new PostgreSqlDataTypeProvider();
    SqlHelper sqlHelper = new SqlHelper(provider, errorHandler);
    PostgreSqlBackendConfiguration configuration = new PostgreSqlBackendConfiguration() {
      @Override
      public long getCursorTimeout() {
        return 10_000;
      }

      @Override
      public long getConnectionPoolTimeout() {
        return 10_000;
      }

      @Override
      public int getConnectionPoolSize() {
        return 30;
      }

      @Override
      public int getReservedReadPoolSize() {
        return 10;
      }

      @Override
      public String getUsername() {
        return "test";
      }

      @Override
      public String getPassword() {
        return "test";
      }

      @Override
      public String getDbHost() {
        return "localhost";
      }

      @Override
      public String getDbName() {
        return "test";
      }

      @Override
      public int getDbPort() {
        return 5432;
      }

      @Override
      public boolean includeForeignKeys() {
        return false;
      }
    };

    DslContextFactory dslContextFactory = new DslContextFactoryImpl(provider);
    SqlInterface sqlInterface =
        buildSqlInterface(provider, sqlHelper, errorHandler, configuration, dslContextFactory);
    SchemaUpdater schemaUpdater = new PostgreSqlSchemaUpdater(sqlInterface, sqlHelper);

    return new PostgreSqlDatabaseTestContext(sqlInterface, dslContextFactory, schemaUpdater);
  }

  private SqlInterface buildSqlInterface(DataTypeProvider provider, SqlHelper sqlHelper,
                                         PostgreSqlErrorHandler errorHandler,
                                         PostgreSqlBackendConfiguration configuration,
                                         DslContextFactory dslContextFactory) {
    PostgreSqlDriverProvider driver = new OfficialPostgreSqlDriver();
    ThreadFactory threadFactory = Executors.defaultThreadFactory();

    IdentifierConstraints identifierConstraints = new PostgreSqlIdentifierConstraints();

    PostgreSqlDbBackend dbBackend = new PostgreSqlDbBackend(threadFactory, configuration, driver, errorHandler);

    PostgreSqlMetaDataReadInterface metaDataReadInterface = new PostgreSqlMetaDataReadInterface(sqlHelper);
    PostgreSqlStructureInterface structureInterface =
        new PostgreSqlStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, identifierConstraints);

    PostgreSqlMetaDataWriteInterface metadataWriteInterface =
        new PostgreSqlMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

    dbBackend.startAsync();
    dbBackend.awaitRunning();

    return new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, provider,
        structureInterface, null, null, identifierConstraints, errorHandler, dslContextFactory, dbBackend);
  }

}
