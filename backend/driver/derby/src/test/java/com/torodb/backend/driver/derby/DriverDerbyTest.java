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

package com.torodb.backend.driver.derby;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class DriverDerbyTest {
    private static final Logger LOGGER = LogManager.getLogger(
            DriverDerbyTest.class
    );
    
    @Test
    public void testDerbySetup() throws Exception {
        LOGGER.warn("Test message");
        
        OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
        DataSource dataSource = derbyDriver.getConfiguredDataSource(new DerbyDbBackendConfiguration() {
            @Override
            public String getUsername() {
                return null;
            }
            
            @Override
            public int getReservedReadPoolSize() {
                return 0;
            }
            
            @Override
            public String getPassword() {
                return null;
            }
            
            @Override
            public int getDbPort() {
                return 0;
            }
            
            @Override
            public String getDbName() {
                return "torodb";
            }
            
            @Override
            public String getDbHost() {
                return null;
            }
            
            @Override
            public long getCursorTimeout() {
                return 0;
            }
            
            @Override
            public long getConnectionPoolTimeout() {
                return 0;
            }
            
            @Override
            public int getConnectionPoolSize() {
                return 0;
            }

            @Override
            public boolean isInMemory() {
                return true;
            }
        }, "torod");
    }
}
