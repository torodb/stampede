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

import com.eightkdata.mongowp.mongoserver.MongoServerConfig;
import com.google.inject.AbstractModule;
import com.torodb.config.model.Config;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.util.MongoDocumentBuilderFactory;

/**
 *
 */
public class ConfigModule extends AbstractModule {
    private final Config config;
    private final MongoServerConfig mongoServerConfig;

    public ConfigModule(Config config) {
    	this.config = config;
        this.mongoServerConfig = new MongoServerConfigMapper(config);
    }
    
    @Override
    protected void configure() {
        bind(MongoServerConfig.class).toInstance(mongoServerConfig);
        bind(DocumentBuilderFactory.class).to(MongoDocumentBuilderFactory.class);
        if (config.getBackend().isPostgresLike()) {
        	bind(String.class).annotatedWith(DatabaseName.class).toInstance(config.getBackend().asPostgres().getDatabase());
        }
    }
    
    private static class MongoServerConfigMapper implements MongoServerConfig {
    	
    	private final Config config;
    	
		public MongoServerConfigMapper(Config config) {
			super();
			this.config = config;
		}

		@Override
		public int getPort() {
			return config.getProtocol().getMongo().getNet().getPort();
		}
    }
}
