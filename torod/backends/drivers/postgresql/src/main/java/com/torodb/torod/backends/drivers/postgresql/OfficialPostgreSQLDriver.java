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

package com.torodb.torod.backends.drivers.postgresql;

import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.db.backends.DbBackendConfiguration;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * A provider for the PostgreSQL driver based on the "official" PostgreSQL driver
 * @see <a href="http://jdbc.postgresql.org/">PostgreSQL JDBC Driver</a>
 */
public class OfficialPostgreSQLDriver implements PostgreSQLDriverProvider {
    @Override
    public DataSource getConfiguredDataSource(DbBackendConfiguration configuration, String poolName) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();

        dataSource.setUser(configuration.getUsername());
        dataSource.setPassword(configuration.getPassword());
        dataSource.setServerName(configuration.getDbHost());
        dataSource.setPortNumber(configuration.getDbPort());
        dataSource.setDatabaseName(configuration.getDbName());

        dataSource.setApplicationName("torodb-" + poolName);

        Statement stat = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            stat = conn.createStatement();
            rs = stat.executeQuery("SELECT 1");
            rs.next();
        } catch (SQLException ex) {
            throw new ToroRuntimeException(ex.getLocalizedMessage());
        } finally {
	            try {	
		            if (rs != null) rs.close();                      
	            } catch (SQLException ex) {	
	            }
	            try {	
		            if (stat != null) stat.close();                      
	        	} catch (SQLException ex) {	
	            } 
	            try {	
	                if (conn != null) conn.close();                      
	            } catch (SQLException ex) {	
	            } 
        }     
        return dataSource;
    }
}
