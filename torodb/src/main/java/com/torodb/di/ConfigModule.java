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
import com.torodb.Config;
import com.torodb.mongowp.mongoserver.api.toro.util.MongoDocumentBuilderFactory;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.config.TorodConfig;

/**
 *
 */
public class ConfigModule extends AbstractModule {
   
    private final Config config;

    public ConfigModule(Config config) {
        this.config = config;
    }
    
    @Override
    protected void configure() {
        bind(TorodConfig.class).toInstance(config);
        bind(MongoServerConfig.class).toInstance(config);
        bind(DocumentBuilderFactory.class).to(MongoDocumentBuilderFactory.class);
        bind(String.class)
                .annotatedWith(DatabaseName.class)
                .toInstance(config.getDbname());
    }
}
