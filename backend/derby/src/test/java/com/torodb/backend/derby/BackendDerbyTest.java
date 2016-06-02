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

package com.torodb.backend.derby;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.torodb.backend.derby.converters.DerbyKVTypeToSqlType;
import com.torodb.backend.driver.derby.DerbyDbBackendConfiguration;
import com.torodb.backend.driver.derby.OfficialDerbyDriver;

public class BackendDerbyTest {
    private static final Logger LOGGER = LogManager.getLogger(
            BackendDerbyTest.class
    );
    
    @Test
    public void testTorodbMeta() throws Exception {
        LOGGER.warn("Test message");
        
        OfficialDerbyDriver derbyDriver = new OfficialDerbyDriver();
        DataSource derbyDataSource = derbyDriver.getConfiguredDataSource(new DerbyDbBackendConfiguration() {
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
        DerbyDatabaseInterface derbyDatabaseInterface = new DerbyDatabaseInterface(new DerbyKVTypeToSqlType());
        
        DerbyTorodbMeta derbyTorodbMeta = new DerbyTorodbMeta(DSL.using(derbyDataSource.getConnection(), SQLDialect.DERBY), derbyDatabaseInterface);
    }
}
