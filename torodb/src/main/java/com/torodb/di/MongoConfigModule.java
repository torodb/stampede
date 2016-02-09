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

import com.eightkdata.mongowp.server.MongoServerConfig;
import com.google.inject.AbstractModule;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.greenplum.Greenplum;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.util.MongoDocumentBuilderFactory;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import javax.inject.Singleton;

public class MongoConfigModule extends AbstractModule implements BackendImplementationVisitor {
	private final Config config;

	public MongoConfigModule(Config config) {
		this.config = config;
	}
	
	@Override
	protected void configure() {
		bind(MongoServerConfig.class).to(MongoServerConfigMapper.class);
		bind(DocumentBuilderFactory.class).to(MongoDocumentBuilderFactory.class);
		config.getBackend().getBackendImplementation().accept(this);
	}

	@Override
	public void visit(Postgres value) {
		bind(String.class).annotatedWith(DatabaseName.class).toInstance(value.getDatabase());
	}

	@Override
	public void visit(Greenplum value) {
		bind(String.class).annotatedWith(DatabaseName.class).toInstance(value.getDatabase());
	}
	
	@Immutable
	@Singleton
	private static class MongoServerConfigMapper implements MongoServerConfig {
		
		private final int port;
		
		@Inject
		public MongoServerConfigMapper(Config config) {
			super();
			this.port = config.getProtocol().getMongo().getNet().getPort();
		}

		@Override
		public int getPort() {
			return port;
		}
	}
}
