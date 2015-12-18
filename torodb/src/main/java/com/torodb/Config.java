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
import com.google.common.net.HostAndPort;
import com.torodb.torod.db.backends.DbBackendConfiguration;
import javax.annotation.Nullable;

/**
 *
 */
public class Config implements DbBackendConfiguration, MongoServerConfig {
	
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

    @Parameter(
            names={"--syncSource"},
            description = "If configured as replication node, the host and "
                    + "port (<host>:<port>) of the node from ToroDB has to "
                    + "replicate. If this node must run as primary, this "
                    + "paramenter must not be defined")
    private String syncSourceAddress;

	@Parameter(names={"--debug"}, description="Change log level to DEBUG")
	private boolean debug = false;
	@Parameter(names={"--verbose"}, description="Change log level to TRACE")
	private boolean verbose = false;

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

	public void setPassword(String password) {
    	this.dbpass = password;
    }
    
    public boolean help() {
    	return help;
    }

    @Nullable
    public HostAndPort getSyncSource() {
        if (syncSourceAddress == null) {
            return null;
        }
        return HostAndPort.fromString(syncSourceAddress);
    }

    // DbBackendConfiguration methods

    @Override
    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    @Override
    public int getReservedReadPoolSize() {
        return reservedReadPoolSize;
    }

    @Override
    public String getUsername() {
        return dbuser;
    }

    @Override
    public String getPassword() {
        return dbpass;
    }

    @Override
    public String getDbHost() {
        return dbhost;
    }

    @Override
    public String getDbName() {
        return dbname;
    }

    @Override
    public int getDbPort() {
        return dbport;
    }


    // MongoServerConfig methods

    @Override
    public int getPort() {
        return mongoPort;
    }
}
