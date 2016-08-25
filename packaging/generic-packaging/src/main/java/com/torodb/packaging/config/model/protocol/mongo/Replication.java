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

package com.torodb.packaging.config.model.protocol.mongo;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.validation.RequiredParametersForAuthentication;

@JsonPropertyOrder({
	"replSetName",
	"role"
})
public class Replication {
	
	@Description("config.protocol.mongo.replication.replSetName")
	@NotEmpty
	@JsonProperty(required=true)
	private String replSetName;
	@Description("config.protocol.mongo.replication.role")
	@NotNull
	@JsonProperty(required=true)
	private Role role = Role.HIDDEN_SLAVE;
	@Description("config.protocol.mongo.replication.syncSource")
    @JsonProperty(required=true)
	private String syncSource;
    @Description("config.protocol.mongo.replication.ssl")
    @NotNull
    @JsonProperty(required=true)
    private SSL ssl = new SSL();
    @Description("config.protocol.mongo.replication.auth")
    @NotNull
    @JsonProperty(required=true)
    @RequiredParametersForAuthentication
    private Auth auth = new Auth();
	
	public String getReplSetName() {
		return replSetName;
	}
	public void setReplSetName(String replSetName) {
		this.replSetName = replSetName;
	}
	public Role getRole() {
		return role;
	}
	public void setRole(Role role) {
		this.role = role;
	}
	public String getSyncSource() {
		return syncSource;
	}
	public void setSyncSource(String syncSource) {
		this.syncSource = syncSource;
	}
    public SSL getSsl() {
        return ssl;
    }
    public void setSsl(SSL ssl) {
        this.ssl = ssl;
    }
    public Auth getAuth() {
        return auth;
    }
    public void setAuth(Auth auth) {
        this.auth = auth;
    }
}
