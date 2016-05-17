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


package com.torodb.di;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.mysql.MySQL;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.torod.backends.drivers.mysql.MySQLDriverProvider;
import com.torodb.torod.backends.drivers.mysql.OfficialMySQLDriver;
import com.torodb.torod.backends.drivers.postgresql.OfficialPostgreSQLDriver;
import com.torodb.torod.backends.drivers.postgresql.PostgreSQLDriverProvider;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.DbBackendConfiguration;
import com.torodb.torod.db.backends.converters.ScalarTypeToSqlType;
import com.torodb.torod.db.backends.greenplum.GreenplumDatabaseInterface;
import com.torodb.torod.db.backends.greenplum.GreenplumDbBackend;
import com.torodb.torod.db.backends.greenplum.GreenplumDbWrapper;
import com.torodb.torod.db.backends.greenplum.converters.GreenplumScalarTypeToSqlType;
import com.torodb.torod.db.backends.mysql.MySQLDatabaseInterface;
import com.torodb.torod.db.backends.mysql.MySQLDbBackend;
import com.torodb.torod.db.backends.mysql.MySQLDbWrapper;
import com.torodb.torod.db.backends.mysql.converters.MySQLScalarTypeToSqlType;
import com.torodb.torod.db.backends.postgresql.PostgreSQLDatabaseInterface;
import com.torodb.torod.db.backends.postgresql.PostgreSQLDbBackend;
import com.torodb.torod.db.backends.postgresql.PostgreSQLDbWrapper;
import com.torodb.torod.db.backends.postgresql.converters.PostgreSQLScalarTypeToSqlType;

public class BackendModule extends AbstractModule implements BackendImplementationVisitor {
	private final Config config;

	public BackendModule(Config config) {
		this.config = config;
	}

	@Override
	protected void configure() {
		config.getBackend().getBackendImplementation().accept(this);
	}

	@Override
	public void visit(Postgres value) {
		bind(DbBackend.class).to(PostgreSQLDbBackend.class).in(Singleton.class);
		bind(DbBackendConfiguration.class).to(PostgresDbBackendConfigurationMapper.class);
		bind(DbWrapper.class).to(PostgreSQLDbWrapper.class).in(Singleton.class);
		bind(PostgreSQLDbWrapper.class).in(Singleton.class);
		bind(DatabaseInterface.class).to(PostgreSQLDatabaseInterface.class).in(Singleton.class);
		bind(ScalarTypeToSqlType.class).to(PostgreSQLScalarTypeToSqlType.class).in(Singleton.class);
		bind(PostgreSQLDriverProvider.class).to(OfficialPostgreSQLDriver.class).in(Singleton.class);
	}

	@Override
	public void visit(Greenplum value) {
        bind(DbBackend.class).to(GreenplumDbBackend.class).in(Singleton.class);
        bind(DbBackendConfiguration.class).to(GreenplumBackendConfigurationMapper.class);
        bind(DbWrapper.class).to(GreenplumDbWrapper.class).in(Singleton.class);
        bind(GreenplumDbWrapper.class).in(Singleton.class);
        bind(DatabaseInterface.class).to(GreenplumDatabaseInterface.class).in(Singleton.class);
        bind(ScalarTypeToSqlType.class).to(GreenplumScalarTypeToSqlType.class).in(Singleton.class);
        bind(PostgreSQLDriverProvider.class).to(OfficialPostgreSQLDriver.class).in(Singleton.class);
	}

    @Override
    public void visit(MySQL value) {
        bind(DbBackend.class).to(MySQLDbBackend.class).in(Singleton.class);
        bind(DbBackendConfiguration.class).to(MySQLDbBackendConfigurationMapper.class);
        bind(DbWrapper.class).to(MySQLDbWrapper.class).in(Singleton.class);
        bind(MySQLDbWrapper.class).in(Singleton.class);
        bind(DatabaseInterface.class).to(MySQLDatabaseInterface.class).in(Singleton.class);
        bind(ScalarTypeToSqlType.class).to(MySQLScalarTypeToSqlType.class).in(Singleton.class);
        bind(MySQLDriverProvider.class).to(OfficialMySQLDriver.class).in(Singleton.class);
    }
    
    @Immutable
    @Singleton
    public static class GreenplumBackendConfigurationMapper extends DbBackendConfigurationMapper {
        @Inject
        public GreenplumBackendConfigurationMapper(Config config, Greenplum greenplum) {
            super(config.getProtocol().getMongo().getCursorTimeout(),
                    config.getGeneric().getConnectionPoolTimeout(),
                    config.getGeneric().getConnectionPoolSize(),
                    config.getGeneric().getReservedReadPoolSize(),
                    greenplum.getHost(),
                    greenplum.getPort(),
                    greenplum.getDatabase(),
                    greenplum.getUser(),
                    greenplum.getPassword());
        }
    }
    
    @Immutable
    @Singleton
    public static class PostgresDbBackendConfigurationMapper extends DbBackendConfigurationMapper {
        @Inject
        public PostgresDbBackendConfigurationMapper(Config config, Postgres postgres) {
            super(config.getProtocol().getMongo().getCursorTimeout(),
                    config.getGeneric().getConnectionPoolTimeout(),
                    config.getGeneric().getConnectionPoolSize(),
                    config.getGeneric().getReservedReadPoolSize(),
                    postgres.getHost(),
                    postgres.getPort(),
                    postgres.getDatabase(),
                    postgres.getUser(),
                    postgres.getPassword());
        }
    }
    
    @Immutable
    @Singleton
    public static class MySQLDbBackendConfigurationMapper extends DbBackendConfigurationMapper {
        @Inject
        public MySQLDbBackendConfigurationMapper(Config config, MySQL mysql) {
            super(config.getProtocol().getMongo().getCursorTimeout(),
                    config.getGeneric().getConnectionPoolTimeout(),
                    config.getGeneric().getConnectionPoolSize(),
                    config.getGeneric().getReservedReadPoolSize(),
                    mysql.getHost(),
                    mysql.getPort(),
                    mysql.getDatabase(),
                    mysql.getUser(),
                    mysql.getPassword());
        }
    }
	
	public static abstract class DbBackendConfigurationMapper implements DbBackendConfiguration {

        private final long cursorTimeout;
        private final long connectionPoolTimeout;
        private final int connectionPoolSize;
        private final int reservedReadPoolSize;
		private final String dbHost;
		private final int dbPort;
		private final String dbName;
		private final String username;
		private final String password;
		
		@Inject
        public DbBackendConfigurationMapper(long cursorTimeout, long connectionPoolTimeout, int connectionPoolSize,
                int reservedReadPoolSize, String dbHost, int dbPort, String dbName, String username, String password) {
            super();
            this.cursorTimeout = cursorTimeout;
            this.connectionPoolTimeout = connectionPoolTimeout;
            this.connectionPoolSize = connectionPoolSize;
            this.reservedReadPoolSize = reservedReadPoolSize;
            this.dbHost = dbHost;
            this.dbPort = dbPort;
            this.dbName = dbName;
            this.username = username;
            this.password = password;
        }

		@Override
        public long getCursorTimeout() {
            return cursorTimeout;
        }

        @Override
        public long getConnectionPoolTimeout() {
            return connectionPoolTimeout;
        }

        @Override
		public int getConnectionPoolSize() {
			return connectionPoolSize;
		}

		@Override
		public int getReservedReadPoolSize() {
			return reservedReadPoolSize;
		}

		@Override
		public String getUsername() {
			return username;
		}

		@Override
		public String getPassword() {
			return password;
		}

		@Override
		public String getDbHost() {
			return dbHost;
		}

		@Override
		public String getDbName() {
			return dbName;
		}

		@Override
		public int getDbPort() {
			return dbPort;
		}
	}
}
