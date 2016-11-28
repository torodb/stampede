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

package com.torodb.packaging.guice;

import com.torodb.backend.driver.postgresql.PostgreSqlBackendConfiguration;
import com.torodb.backend.postgresql.guice.PostgreSqlBackendModule;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

public class BackendPostgresImplementationModule
    extends BackendImplementationModule<AbstractPostgres, PostgreSqlBackendConfiguration> {

  public BackendPostgresImplementationModule() {
    super(AbstractPostgres.class,
        PostgreSqlBackendConfiguration.class,
        PostgresSqlDbBackendConfigurationMapper.class,
        () -> new PostgreSqlBackendModule());
  }

  @Immutable
  @ThreadSafe
  public static class PostgresSqlDbBackendConfigurationMapper 
      extends BackendConfigurationMapper implements PostgreSqlBackendConfiguration {

    @Inject
    public PostgresSqlDbBackendConfigurationMapper(CursorConfig cursorConfig,
        ConnectionPoolConfig connectionPoolConfig, AbstractPostgres postgres) {
      super(cursorConfig.getCursorTimeout(),
          connectionPoolConfig.getConnectionPoolTimeout(),
          connectionPoolConfig.getConnectionPoolSize(),
          connectionPoolConfig.getReservedReadPoolSize(),
          postgres.getHost(),
          postgres.getPort(),
          postgres.getDatabase(),
          postgres.getUser(),
          postgres.getPassword(),
          postgres.getIncludeForeignKeys());
    }
  }
}
