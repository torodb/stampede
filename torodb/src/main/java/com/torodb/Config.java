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

package com.torodb;

import com.beust.jcommander.Parameter;
import com.eightkdata.mongowp.mongoserver.MongoServerConfig;
import com.google.common.base.Preconditions;
import com.torodb.torod.core.config.TorodConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 *
 */
public class Config implements TorodConfig, MongoServerConfig {
	
	@Parameter(names={"--help","--usage"}, description="Print this usage guide")
	private boolean help = false;
	@Parameter(names={"-h","--host"}, description="PostgreSQL's server host (hostname or IP address)")
	private String dbhost = "localhost";
	@Parameter(names={"-p","--dbport"}, description="PostgreSQL's server port")
	private Integer dbport = 5432;
	@Parameter(names={"-d","--dbname"}, description="PostgreSQL's database name to connect to (must exist)")
	private String dbname = "torod";
	@Parameter(names={"-u","--username"}, description="PostgreSQL's database user name. Must be a superuser")
	private String dbuser = "postgres";
	@Parameter(names={"--ask-for-password"}, description="Force input of PostgreSQL's database user password.")
	private boolean askForPassword = false;
	private String dbpass = null;
	@Parameter(
            names={"-c","--total-connections"}, 
            description="Maximum number of connections to establish to the "
                    + "PostgreSQL database. It must be higher or equal than 3")
    private Integer connectionPoolSize = 30;
    @Parameter(
            names={"-r","--read-connections"}, 
            description="Reserved connections that will be reserved to store "
                    + "global cursors. It must be lower than total connections "
                    + "minus 2")
    private Integer reservedReadPoolSize = 10;
	@Parameter(names={"-P", "--mongoport"}, description="Port to listen on for Mongo wire protocol connections")
    private int mongoPort = 27017;
	@Parameter(names={"--debug"}, description="Change log level to DEBUG")
	private boolean debug = false;
	@Parameter(names={"--verbose"}, description="Change log level to INFO")
	private boolean verbose = false;

    private DataSource commonDataSource;
    private DataSource systemDataSource;
    private DataSource globalCursorDataSource;
    
    public String getDbhost() {
		return dbhost;
	}

	public Integer getDbport() {
		return dbport;
	}

	public String getDbname() {
		return dbname;
	}

	public String getDbuser() {
		return dbuser;
	}

	public boolean askForPassword() {
    	return askForPassword;
    }

	public boolean hasPassword() {
    	return dbpass != null;
    }
	
	public boolean debug() {
		return debug;
	}
	
	public boolean verbose() {
		return verbose;
	}
    
    public void initialize() {
        Preconditions.checkState(
                connectionPoolSize >= 3, 
                "At least three connections with the backend SQL database are "
                        + "required"
        );
        Preconditions.checkState(
                reservedReadPoolSize >= 1,
                "At least one read connection is required"
        );
        Preconditions.checkState(
                reservedReadPoolSize <= connectionPoolSize -2, 
                "Reserved read connections must be lower than total "
                        + "connections minus two"
        );
        
        commonDataSource = createDataSource(connectionPoolSize - reservedReadPoolSize);
        systemDataSource = createDataSource(1);
        globalCursorDataSource = createDataSource(reservedReadPoolSize);
    }
    
    private DataSource createDataSource(int maxPoolSize) {
        PGSimpleDataSource pgds = new PGSimpleDataSource();
        pgds.setServerName(dbhost);
        pgds.setPortNumber(dbport);
        pgds.setDatabaseName(dbname);
        pgds.setUser(dbuser);
        pgds.setPassword(dbpass);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setDataSource(pgds);

        return new HikariDataSource(hikariConfig);
    }
    
    public void shutdown() {
    	((HikariDataSource) commonDataSource).shutdown();
    }
    
    @Override
	public int getPort() {
		return mongoPort;
	}

	public void setPassword(String password) {
    	this.dbpass = password;
    }
    
    public boolean help() {
    	return help;
    }

    @Override
    public DataSource getSessionDataSource() {
        return commonDataSource;
    }

    @Override
    public DataSource getSystemDataSource() {
        return systemDataSource;
    }

    @Override
    public DataSource getGlobalCursorDatasource() {
        return globalCursorDataSource;
    }

    @Override
    public int getByJobDependencyStripes() {
        return 16;
    }

    @Override
    public int getCacheSubDocTypeStripes() {
        return 64;
    }

    @Override
    public long getDefaultCursorTimeout() {
        return 10 * 60 * 1000;
    }

}
