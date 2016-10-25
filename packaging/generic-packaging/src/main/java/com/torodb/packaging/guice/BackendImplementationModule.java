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

import com.google.inject.PrivateModule;
import com.torodb.backend.DbBackendConfiguration;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.SqlHelper;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.postgresql.PostgreSQLDbBackendConfiguration;
import com.torodb.backend.guice.BackendModule;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.postgresql.guice.PostgreSQLBackendModule;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.backend.SnapshotUpdater;
import com.torodb.core.d2r.ReservedIdGenerator;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.model.backend.derby.Derby;
import com.torodb.packaging.config.model.backend.postgres.Postgres;
import com.torodb.packaging.config.visitor.BackendImplementationVisitor;

public class BackendImplementationModule extends PrivateModule implements BackendImplementationVisitor {
    private final CursorConfig cursorConfig;
    private final ConnectionPoolConfig connectionPoolConfig;
	private final BackendImplementation backendImplementation;

	public BackendImplementationModule(CursorConfig cursorConfig, ConnectionPoolConfig connectionPoolConfig, 
	        BackendImplementation backendImplementation) {
	    this.cursorConfig = cursorConfig;
	    this.connectionPoolConfig = connectionPoolConfig;
		this.backendImplementation = backendImplementation;
	}

	@Override
	protected void configure() {
        bind(CursorConfig.class).toInstance(cursorConfig);
        bind(ConnectionPoolConfig.class).toInstance(connectionPoolConfig);
        install(new BackendModule());
	    backendImplementation.accept(this);
        expose(SqlHelper.class);
        expose(SqlInterface.class);
        expose(SchemaUpdater.class);
        expose(DbBackendService.class);
        expose(IdentifierConstraints.class);
        expose(BackendBundleFactory.class);
        expose(BackendTransactionJobFactory.class);
        expose(ReservedIdGenerator.class);
        expose(SnapshotUpdater.class);
	}

	@Override
	public void visit(Postgres value) {
        bind(Postgres.class).toInstance(value);
        bind(PostgreSQLDbBackendConfiguration.class).to(PostgresSQLDbBackendConfigurationMapper.class);
		install(new PostgreSQLBackendModule());
	}

	@Override
	public void visit(Derby value) {
        bind(Derby.class).toInstance(value);
        bind(DerbyDbBackendConfiguration.class).to(DerbyBackendConfigurationMapper.class);
        install(new DerbyBackendModule());
	}
    
    @Immutable
    @ThreadSafe
    public static class DerbyBackendConfigurationMapper extends DbBackendConfigurationMapper implements DerbyDbBackendConfiguration {
        private final boolean embedded;
        private final boolean inMemory;
        
        @Inject
        public DerbyBackendConfigurationMapper(CursorConfig cursorConfig, ConnectionPoolConfig connectionPoolConfig, Derby derby) {
            super(cursorConfig.getCursorTimeout(),
                    connectionPoolConfig.getConnectionPoolTimeout(),
                    connectionPoolConfig.getConnectionPoolSize(),
                    connectionPoolConfig.getReservedReadPoolSize(),
                    derby.getHost(),
                    derby.getPort(),
                    derby.getDatabase(),
                    derby.getUser(),
                    derby.getPassword(),
                    derby.getIncludeForeignKeys());
            
            this.embedded = derby.getEmbedded();
            this.inMemory = derby.getInMemory();
        }

        @Override
        public boolean embedded() {
            return embedded;
        }

        @Override
        public boolean inMemory() {
            return inMemory;
        }
    }
    
    @Immutable
    public static class PostgresSQLDbBackendConfigurationMapper extends DbBackendConfigurationMapper implements PostgreSQLDbBackendConfiguration {
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

    @ThreadSafe
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
		private boolean includeForeignKeys;
		
		@Inject
        public DbBackendConfigurationMapper(long cursorTimeout, long connectionPoolTimeout, int connectionPoolSize,
                int reservedReadPoolSize, String dbHost, int dbPort, String dbName, String username, String password,
                boolean includeForeignKeys) {
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
            this.includeForeignKeys = includeForeignKeys;
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

        @Override
        public boolean includeForeignKeys() {
            return includeForeignKeys;
        }
	}
}
