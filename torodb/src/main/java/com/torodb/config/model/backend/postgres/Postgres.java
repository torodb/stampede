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

package com.torodb.config.model.backend.postgres;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.annotation.Description;
import com.torodb.config.model.backend.BackendImplementation;
import com.torodb.config.model.backend.Password;
import com.torodb.config.validation.ExistsAnyPassword;
import com.torodb.config.validation.Host;
import com.torodb.config.validation.Port;
import com.torodb.config.visitor.BackendImplementationVisitor;

@Description("config.backend.postgres")
@JsonPropertyOrder({
	"host",
	"port",
	"database",
	"user",
	"password",
	"toropassFile",
	"applicationName"
})
@ExistsAnyPassword
public class Postgres implements BackendImplementation, Password {
	@Description("config.backend.postgres.host")
	@NotNull
	@Host
	@JsonProperty(required=true)
	private String host = "localhost";
	@Description("config.backend.postgres.port")
	@NotNull
	@Port
	@JsonProperty(required=true)
	private Integer port = 5432;
	@Description("config.backend.postgres.database")
	@Deprecated
	@NotNull
	@JsonProperty(required=true)
	private String database = "torod";
	@Description("config.backend.postgres.user")
	@NotNull
	@JsonProperty(required=true)
	private String user = "torodb";
	@JsonIgnore
	private String password;
	@Description("config.backend.postgres.toropassFile")
	private String toropassFile = System.getProperty("user.home") + "/.toropass";
	@Description("config.backend.postgres.applicationName")
	@NotNull
	@JsonProperty(required=true)
	private String applicationName = "toro";
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getToropassFile() {
		return toropassFile;
	}
	public void setToropassFile(String toropassFile) {
		this.toropassFile = toropassFile;
	}
	@Deprecated
	public String getDatabase() {
		return database;
	}
	@Deprecated
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getApplicationName() {
		return applicationName;
	}
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}
	
	@Override
	public void accept(BackendImplementationVisitor visitor) {
		visitor.visit(this);
	}
}
