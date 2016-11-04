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


package com.torodb.packaging.guice;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import com.torodb.backend.driver.postgresql.PostgreSQLBackendConfiguration;
import com.torodb.backend.postgresql.guice.PostgreSQLBackendModule;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.model.backend.postgres.Postgres;

public class BackendPostgresImplementationModule extends BackendImplementationModule<Postgres, PostgreSQLBackendConfiguration> {

	public BackendPostgresImplementationModule() {
        super(Postgres.class, 
                PostgreSQLBackendConfiguration.class, 
                PostgresSQLDbBackendConfigurationMapper.class, 
                () -> new PostgreSQLBackendModule());
    }
    
    @Immutable
    @ThreadSafe
    public static class PostgresSQLDbBackendConfigurationMapper extends BackendConfigurationMapper implements PostgreSQLBackendConfiguration {
        @Inject
        public PostgresSQLDbBackendConfigurationMapper(CursorConfig cursorConfig, ConnectionPoolConfig connectionPoolConfig, Postgres postgres) {
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
