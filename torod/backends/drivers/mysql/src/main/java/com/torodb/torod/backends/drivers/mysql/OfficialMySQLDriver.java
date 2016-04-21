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

package com.torodb.torod.backends.drivers.mysql;

import javax.sql.DataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.torodb.torod.db.backends.DbBackendConfiguration;

/**
 *
 * A provider for the PostgreSQL driver based on the "official" PostgreSQL driver
 * @see <a href="http://jdbc.postgresql.org/">PostgreSQL JDBC Driver</a>
 */
public class OfficialMySQLDriver implements MySQLDriverProvider {
    @Override
    public DataSource getConfiguredDataSource(DbBackendConfiguration configuration, String poolName) {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL("jdbc:mysql://" + configuration.getDbHost() + ":" + configuration.getDbPort() + "/" + configuration.getDbName());
        dataSource.setUser(configuration.getUsername());
        dataSource.setPassword(configuration.getPassword());
        dataSource.setConnectionAttributes("program_name:" + "torodb-" + poolName);
        dataSource.setUseInformationSchema(true);
        
        //TODO
//        Statement stat = null;
//        ResultSet rs = null;
//        Connection conn = null;
//        try {
//            conn = dataSource.getConnection();
//            stat = conn.createStatement();
//            rs = stat.executeQuery("SELECT 1");
//            rs.next();
//        } catch (SQLException ex) {
//            throw new ToroRuntimeException(ex.getLocalizedMessage());
//        } finally {
//	            try {
//		            if (rs != null) rs.close();
//	            } catch (SQLException ex) {
//	            }
//	            try {
//		            if (stat != null) stat.close();
//	        	} catch (SQLException ex) {
//	            }
//	            try {
//	                if (conn != null) conn.close();
//	            } catch (SQLException ex) {
//	            }
//        }     
        return dataSource;
    }
}
