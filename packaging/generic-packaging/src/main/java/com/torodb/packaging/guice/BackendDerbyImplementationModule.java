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

import com.torodb.backend.derby.guice.DerbyBackendModule;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.model.backend.derby.Derby;

public class BackendDerbyImplementationModule extends BackendImplementationModule<Derby, DerbyDbBackendConfiguration> {
	
	public BackendDerbyImplementationModule() {
        super(
                Derby.class, 
                DerbyDbBackendConfiguration.class, 
                DerbyBackendConfigurationMapper.class,
                () -> new DerbyBackendModule());
    }

    @Immutable
    @ThreadSafe
    public static class DerbyBackendConfigurationMapper extends BackendConfigurationMapper implements DerbyDbBackendConfiguration {
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
    
}