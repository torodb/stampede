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
import com.torodb.torod.backends.drivers.postgresql.OfficialPostgreSQLDriver;
import com.torodb.torod.backends.drivers.postgresql.PostgreSQLDriverProvider;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.db.backends.DatabaseInterface;
import com.torodb.torod.db.backends.DbBackendConfiguration;
import com.torodb.torod.db.backends.converters.BasicTypeToSqlType;
import com.torodb.torod.db.backends.postgresql.PostgreSQLDbBackend;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.db.backends.postgresql.PostgresqlDatabaseInterface;
import com.torodb.torod.db.backends.postgresql.PostgresqlDbWrapper;
import com.torodb.torod.db.backends.postgresql.converters.PostgresBasicTypeToSqlType;

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
		bind(DbBackendConfiguration.class).to(DbBackendConfigurationMapper.class);
		bind(DbWrapper.class).to(PostgresqlDbWrapper.class).in(Singleton.class);
		bind(PostgresqlDbWrapper.class).in(Singleton.class);
		bind(DatabaseInterface.class).to(PostgresqlDatabaseInterface.class).in(Singleton.class);
		bind(BasicTypeToSqlType.class).to(PostgresBasicTypeToSqlType.class).in(Singleton.class);
		bind(PostgreSQLDriverProvider.class).to(OfficialPostgreSQLDriver.class).in(Singleton.class);
	}

	@Override
	public void visit(Greenplum value) {
		throw new UnsupportedOperationException("Not implemented yet! :(");
	}
	
	@Immutable
	@Singleton
	public static class DbBackendConfigurationMapper implements DbBackendConfiguration {

		private final int connectionPoolSize;
		private final int reservedReadPoolSize;
		private final String dbHost;
		private final int dbPort;
		private final String dbName;
		private final String username;
		private final String password;
		
		@Inject
		public DbBackendConfigurationMapper(Config config, Postgres postgres) {
			super();
			this.connectionPoolSize = config.getGeneric().getConnectionPoolSize();
			this.reservedReadPoolSize = config.getGeneric().getReservedReadPoolSize();
			this.dbHost = postgres.getHost();
			this.dbPort = postgres.getPort();
			this.dbName = postgres.getDatabase();
			this.username = postgres.getUser();
			this.password = postgres.getPassword();
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
