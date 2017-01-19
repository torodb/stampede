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
import com.torodb.backend.common.DatabaseTestContext;
import com.torodb.backend.common.IntegrationTestBundleConfig;
import com.torodb.backend.derby.schema.DerbySchemaUpdater;
import com.torodb.backend.driver.derby.DerbyDbBackendConfig;
import com.torodb.backend.driver.derby.DerbyDbBackendConfigBuilder;
import com.torodb.backend.driver.derby.DerbyDriverProvider;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.modules.BundleConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DerbyDatabaseTestContextFactory {

  public DatabaseTestContext createInstance() {
    BundleConfig bundleConfig = new IntegrationTestBundleConfig();
    DerbyDbBackendConfig configuration = new DerbyDbBackendConfigBuilder(bundleConfig)
        .setDbName("torod")
        .setIncludeForeignKeys(false)
        .build();

    DerbyErrorHandler errorHandler = new DerbyErrorHandler();
    DataTypeProvider provider = new DerbyDataTypeProvider();
    SqlHelper sqlHelper = new SqlHelper(provider, errorHandler);

    DslContextFactory dslContextFactory = new DslContextFactoryImpl(provider);
    SqlInterface sqlInterface =
        buildSqlInterface(provider, sqlHelper, errorHandler, configuration, dslContextFactory);
    SchemaUpdater schemaUpdater = new DerbySchemaUpdater(sqlInterface, sqlHelper);

    return new DerbyDatabaseTestContext(sqlInterface, dslContextFactory, schemaUpdater, configuration);
  }

  private SqlInterface buildSqlInterface(DataTypeProvider provider, SqlHelper sqlHelper,
                                         DerbyErrorHandler errorHandler,
                                         DerbyDbBackendConfig configuration,
                                         DslContextFactory dslContextFactory) {
    DerbyDriverProvider driver = new OfficialDerbyDriver();
    ThreadFactory threadFactory = Executors.defaultThreadFactory();

    IdentifierConstraints identifierConstraints = new DerbyIdentifierConstraints();

    DerbyDbBackend dbBackend = new DerbyDbBackend(threadFactory, configuration, driver, errorHandler);

    DerbyMetaDataReadInterface metaDataReadInterface = new DerbyMetaDataReadInterface(sqlHelper);
    DerbyStructureInterface derbyStructureInterface =
        new DerbyStructureInterface(dbBackend, metaDataReadInterface, sqlHelper, identifierConstraints);

    DerbyMetaDataWriteInterface metadataWriteInterface =
        new DerbyMetaDataWriteInterface(metaDataReadInterface, sqlHelper);

    dbBackend.startAsync();
    dbBackend.awaitRunning();

    return new SqlInterfaceDelegate(metaDataReadInterface, metadataWriteInterface, provider,
        derbyStructureInterface, null, null, identifierConstraints, errorHandler, dslContextFactory, dbBackend);
  }

}