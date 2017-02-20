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

package com.torodb.packaging.config.util;

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.inject.Injector;
import com.torodb.backend.BackendConfig;
import com.torodb.backend.BackendConfigImplBuilder;
import com.torodb.backend.derby.DerbyDbBackendBundle;
import com.torodb.backend.driver.derby.DerbyDbBackendConfig;
import com.torodb.backend.driver.derby.DerbyDbBackendConfigBuilder;
import com.torodb.backend.postgresql.PostgreSqlBackendBundle;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.packaging.config.model.backend.AbstractBackend;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.derby.AbstractDerby;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.util.MongoClientConfigurationFactory;
import com.torodb.packaging.util.ReplicationFiltersFactory;
import com.torodb.torod.SqlTorodBundle;
import com.torodb.torod.SqlTorodConfig;
import com.torodb.torod.TorodBundle;

public class BundleFactory {
  private BundleFactory() {}

  /**
   * Creates a {@link BackendBundle} configured with the given {@link AbstractBackend}.
   */
  public static BackendBundle createBackendBundle(AbstractBackend backend,
      BundleConfig generalConfig) {

    ConnectionPoolConfig connPoolConf = backend.getConnectionPoolConfig();

    return backend.getBackendImplementation()
        .accept(new BackendImplementationVisitor<BackendBundle, Void>() {
          @Override
          public BackendBundle visit(AbstractPostgres value, Void arg) {
            BackendConfig config = new BackendConfigImplBuilder(generalConfig)
                .setConnectionPoolSize(connPoolConf.getConnectionPoolSize())
                .setConnectionPoolTimeout(connPoolConf.getConnectionPoolTimeout())
                .setDbHost(value.getHost())
                .setDbName(value.getDatabase())
                .setDbPort(value.getPort())
                .setIncludeForeignKeys(value.getIncludeForeignKeys())
                .setPassword(value.getPassword())
                .setReservedReadPoolSize(connPoolConf.getReservedReadPoolSize())
                .setUsername(value.getUser())
                .build();
            return new PostgreSqlBackendBundle(config);
          }

          @Override
          public BackendBundle visit(AbstractDerby value, Void arg) {
            DerbyDbBackendConfig config = new DerbyDbBackendConfigBuilder(generalConfig)
                .setConnectionPoolSize(connPoolConf.getConnectionPoolSize())
                .setConnectionPoolTimeout(connPoolConf.getConnectionPoolTimeout())
                .setDbHost(value.getHost())
                .setDbName(value.getDatabase())
                .setDbPort(value.getPort())
                .setIncludeForeignKeys(value.getIncludeForeignKeys())
                .setPassword(value.getPassword())
                .setReservedReadPoolSize(connPoolConf.getReservedReadPoolSize())
                .setUsername(value.getUser())

                .setInMemory(value.getInMemory())
                .setEmbedded(value.getEmbedded())
                .build();
            return new DerbyDbBackendBundle(config);

          }
        }, null);
  }

  /**
   * Creates a {@link TorodBundle} using the given configuration.
   */
  public static TorodBundle createTorodBundle(
      BundleConfig generalConfig,
      BackendBundle backendBundle) {
    Injector essentialInjector = generalConfig.getEssentialInjector();
    return new SqlTorodBundle(new SqlTorodConfig(
        backendBundle,
        essentialInjector, 
        generalConfig.getSupervisor()
    ));
  }

  public static MongoDbReplConfigBuilder createMongoDbReplConfigBundle(
      AbstractReplication replicationConfig,
      BundleConfig generalConfig) {
    @SuppressWarnings("checkstyle:LineLength")
    MongoClientConfiguration mongoClientConfiguration = MongoClientConfigurationFactory.getMongoClientConfiguration(replicationConfig);
    ReplicationFilters replFilters = ReplicationFiltersFactory.getReplicationFilters(
        replicationConfig);
    return new MongoDbReplConfigBuilder(generalConfig)
        .setMongoClientConfiguration(mongoClientConfiguration)
        .setReplicationFilters(replFilters)
        .setReplSetName(replicationConfig.getReplSetName()
        );
  }
}
