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

import com.torodb.backend.*;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DatabaseTestContext {

  private static final Logger LOGGER = LogManager.getLogger(DatabaseTestContext.class);

  private SqlInterface sqlInterface;

  private DslContextFactory dslContextFactory;

  private SchemaUpdater schemaUpdater;

  private DerbyDbBackendConfiguration configuration = new LocalTestDerbyDbBackendConfiguration();

  public DatabaseTestContext() {
    DerbyErrorHandler errorHandler = new DerbyErrorHandler();
    DataTypeProvider provider = new DerbyDataTypeProvider();
    SqlHelper sqlHelper = new SqlHelper(provider, errorHandler);

    sqlInterface = buildSqlInterface(provider, sqlHelper, errorHandler);
    dslContextFactory = buildDslContextFactory(provider);
    schemaUpdater = buildSchemaUpdater(sqlInterface, sqlHelper);
  }

  public void tearDownDatabase() {
    try {
      sqlInterface.getDbBackend().stopAsync();
      sqlInterface.getDbBackend().awaitTerminated();

      DriverManager.getConnection("jdbc:derby:memory:" + configuration.getDbName() + ";drop=true").close();
    } catch (SQLException e) {
      LOGGER.info("Database dropped");
    }
  }

  private SqlInterface buildSqlInterface(DataTypeProvider provider, SqlHelper sqlHelper, DerbyErrorHandler errorHandler) {
    DerbyDriverProvider driver = new OfficialDerbyDriver();
    ThreadFactory threadFactory = Executors.defaultThreadFactory();

    IdentifierConstraints constraints = new DerbyIdentifierConstraints();

    DerbyDbBackend dbBackend = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);

    DerbyMetaDataReadInterface metaDataReadInterface = new DerbyMetaDataReadInterface(sqlHelper);
    DerbyStructureInterface derbyStructureInterface = new DerbyStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, constraints);

    DerbyMetaDataWriteInterface metadataWriteInterface = new DerbyMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

    dbBackend.startAsync();
    dbBackend.awaitRunning();

    return new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, provider, derbyStructureInterface,
        null, null, null, errorHandler, dslContextFactory, dbBackend);
  }

  private DslContextFactory buildDslContextFactory(DataTypeProvider provider) {
    return new DslContextFactoryImpl(provider);
  }

  private SchemaUpdater buildSchemaUpdater(SqlInterface sqlInterface, SqlHelper sqlHelper) {
    return new DerbySchemaUpdater(sqlInterface, sqlHelper);
  }

  public SchemaUpdater getSchemaUpdater() {
    return schemaUpdater;
  }

  public SqlInterface getSqlInterface() {
    return sqlInterface;
  }

  public DslContextFactory getDslContextFactory() {
    return dslContextFactory;
  }

}
