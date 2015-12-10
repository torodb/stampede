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

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.torod.backend.db.DbBackendConfiguration;
import com.torodb.torod.backend.db.postgresql.PostgreSQLDbBackend;
import com.torodb.torod.core.backend.DbBackend;

/**
 *
 */
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
		bind(DbBackendConfiguration.class).toInstance(new DbBackendConfigurationMapper(config, value));
	}

	@Override
	public void visit(Greenplum value) {
		throw new UnsupportedOperationException("Not implemented yet! :(");
	}
    
    public static class DbBackendConfigurationMapper implements DbBackendConfiguration {

    	private final Config config;
    	private final Postgres postgres;
    	
		public DbBackendConfigurationMapper(Config config, Postgres postgres) {
			super();
			this.config = config;
			this.postgres = postgres;
		}

		@Override
		public int getConnectionPoolSize() {
			return config.getGeneric().getConnectionPoolSize();
		}

		@Override
		public int getReservedReadPoolSize() {
			return config.getGeneric().getReservedReadPoolSize();
		}

		@Override
		public String getUsername() {
			return postgres.getUser();
		}

		@Override
		public String getPassword() {
			return postgres.getPassword();
		}

		@Override
		public String getDbHost() {
			return postgres.getHost();
		}

		@Override
		public String getDbName() {
			return postgres.getDatabase();
		}

		@Override
		public int getDbPort() {
			return postgres.getPort();
		}
    }
}
