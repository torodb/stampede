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


package com.torodb.guice;

import com.google.inject.AbstractModule;
import com.torodb.DefaultBuildProperties;
import com.torodb.config.model.Config;
import com.torodb.config.model.backend.derby.Derby;
import com.torodb.config.model.backend.postgres.Postgres;
import com.torodb.config.visitor.BackendImplementationVisitor;
import com.torodb.core.BuildProperties;

public class ConfigModule extends AbstractModule implements BackendImplementationVisitor {
	private final Config config;

	public ConfigModule(Config config) {
		this.config = config;
	}
	
	@Override
	protected void configure() {
        bind(BuildProperties.class).to(DefaultBuildProperties.class).asEagerSingleton();
		bind(Config.class).toInstance(config);
		
		config.getBackend().getBackendImplementation().accept(this);
	}

	@Override
	public void visit(Postgres value) {
		bind(Postgres.class).toInstance(value);
	}

    @Override
    public void visit(Derby value) {
        bind(Derby.class).toInstance(value);
    }
}
