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

package com.torodb.packaging.config.model.backend.derby;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.BackendImplementation;
import com.torodb.packaging.config.model.backend.BackendPasswordConfig;
import com.torodb.packaging.config.validation.ExistsAnyPassword;
import com.torodb.packaging.config.validation.Host;
import com.torodb.packaging.config.validation.InMemoryOnlyIfEmbedded;
import com.torodb.packaging.config.validation.Port;
import com.torodb.packaging.config.visitor.BackendImplementationVisitor;

@Description("config.backend.derby")
@JsonPropertyOrder({
	"host",
	"port",
	"database",
	"user",
	"password",
	"toropassFile",
	"applicationName",
    "embedded",
    "inMemory"
})
@ExistsAnyPassword
@InMemoryOnlyIfEmbedded
public class Derby implements BackendImplementation, BackendPasswordConfig {
	@Description("config.backend.postgres.host")
	@NotNull
	@Host
	@JsonProperty(required=true)
	protected String host = "localhost";
	@Description("config.backend.postgres.port")
	@NotNull
	@Port
	@JsonProperty(required=true)
	protected Integer port = 1527;
	@Description("config.backend.postgres.database")
	@NotNull
	@JsonProperty(required=true)
	protected String database = "torod";
	@Description("config.backend.postgres.user")
	@NotNull
	@JsonProperty(required=true)
	protected String user = "torodb";
	@JsonIgnore
	protected String password;
	@Description("config.backend.postgres.toropassFile")
	protected String toropassFile = System.getProperty("user.home") + "/.toropass";
	@Description("config.backend.postgres.applicationName")
    @NotNull
    @JsonProperty(required=true)
    protected String applicationName = "toro";
    @Description("config.backend.postgres.includeForeignKeys")
    @NotNull
    @JsonProperty(required=true)
    protected Boolean includeForeignKeys = false;
    @Description("config.backend.derby.embedded")
    @NotNull
    @JsonProperty(required=true)
    protected Boolean embedded = true;
    @Description("config.backend.derby.inMemory")
    @NotNull
    @JsonProperty(required=true)
    protected Boolean inMemory = true;
	
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
    @Override
	public String getPassword() {
		return password;
	}
    @Override
	public void setPassword(String password) {
		this.password = password;
	}
    @Override
	public String getToropassFile() {
		return toropassFile;
	}
	public void setToropassFile(String toropassFile) {
		this.toropassFile = toropassFile;
	}
    public void setEmbedded(Boolean embedded) {
        this.embedded = embedded;
    }
    public Boolean getEmbedded() {
        return embedded;
    }
    public void setInMemory(Boolean inMemory) {
        this.inMemory = inMemory;
    }
    public Boolean getInMemory() {
        return inMemory;
    }
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getApplicationName() {
		return applicationName;
	}
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}
    public void setIncludeForeignKeys(Boolean includeForeignKeys) {
        this.includeForeignKeys = includeForeignKeys;
    }
    public Boolean getIncludeForeignKeys() {
        return includeForeignKeys;
    }
	
	@Override
	public void accept(BackendImplementationVisitor visitor) {
		visitor.visit(this);
	}
}
