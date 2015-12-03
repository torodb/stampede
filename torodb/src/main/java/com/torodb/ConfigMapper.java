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

import com.eightkdata.mongowp.mongoserver.MongoServerConfig;
import com.torodb.config.Config;
import com.torodb.config.backend.greenplum.Greenplum;
import com.torodb.config.backend.postgres.Postgres;
import com.torodb.config.protocol.mongo.Replication;
import com.torodb.torod.backend.db.DbBackendConfiguration;

public class ConfigMapper implements DbBackendConfiguration, MongoServerConfig {

	private final String dbhost;
	private final Integer dbport;
	private final String dbname;
	private final String dbuser;
	private final String dbpass;
	private final Integer connectionPoolSize;
	private final Integer reservedReadPoolSize;
	private final int mongoPort;
	private final String syncSource;

	public static ConfigMapper create(Config config) {
		ConfigMapper configMapper = null;

		Replication replication = null;
		
		if (config.getProtocol().getMongo().getReplication() != null &&
				!config.getProtocol().getMongo().getReplication().isEmpty()) {
			replication = config.getProtocol().getMongo().getReplication().get(0);
		}
		
		if (config.getBackend().isPostgres()) {
			Postgres postgres = config.getBackend().asPostgres();

			configMapper = new ConfigMapper(postgres.getHost(), postgres.getPort(), postgres.getDatabase(),
					postgres.getUser(), postgres.getPassword(), config.getGeneric().getConnectionPoolSize(),
					config.getGeneric().getReserverdReadPoolSize(), config.getProtocol().getMongo().getNet().getPort(),
					replication!=null?replication.getSyncSource():null);
		} else 
		if (config.getBackend().isGreenplum()) {
			Greenplum greenplum = config.getBackend().asGreenplum();

			configMapper = new ConfigMapper(greenplum.getHost(), greenplum.getPort(), greenplum.getDatabase(),
					greenplum.getUser(), greenplum.getPassword(), config.getGeneric().getConnectionPoolSize(),
					config.getGeneric().getReserverdReadPoolSize(), config.getProtocol().getMongo().getNet().getPort(),
					replication!=null?replication.getSyncSource():null);
		}

		return configMapper;
	}

	private ConfigMapper(String dbhost, Integer dbport, String dbname, String dbuser, String dbpass,
			Integer connectionPoolSize, Integer reservedReadPoolSize, int mongoPort, String syncSource) {
		super();

		this.dbhost = dbhost;
		this.dbport = dbport;
		this.dbname = dbname;
		this.dbuser = dbuser;
		this.dbpass = dbpass;
		this.connectionPoolSize = connectionPoolSize;
		this.reservedReadPoolSize = reservedReadPoolSize;
		this.mongoPort = mongoPort;
		this.syncSource = syncSource;
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

	public String getSyncSource() {
		return syncSource;
	}
}
