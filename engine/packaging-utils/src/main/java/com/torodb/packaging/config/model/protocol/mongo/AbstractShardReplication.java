/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.packaging.config.model.protocol.mongo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.jackson.RoleWithDefaultDeserializer;
import com.torodb.packaging.config.model.common.EnumWithDefault;
import com.torodb.packaging.config.model.common.StringWithDefault;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"name", "replSetName", "syncSource", "role", 
    "ssl", "auth", "include", "exclude"})
public abstract class AbstractShardReplication {

  private StringWithDefault name = StringWithDefault.withDefault(null);
  private StringWithDefault replSetName = StringWithDefault.withDefault(null);
  private EnumWithDefault<Role> role = EnumWithDefault.withDefault(Role.HIDDEN_SLAVE);
  private StringWithDefault syncSource = StringWithDefault.withDefault(null);
  private Ssl ssl = new Ssl();
  private Auth auth = new Auth();

  @Description("config.mongo.replication.shard.name")
  @JsonProperty(required = true)
  public StringWithDefault getName() {
    return name;
  }

  public void setName(StringWithDefault name) {
    this.name = name;
  }

  @Description("config.mongo.replication.replSetName")
  @JsonProperty(required = true)
  public StringWithDefault getReplSetName() {
    return replSetName;
  }

  public void setReplSetName(StringWithDefault replSetName) {
    this.replSetName = replSetName;
  }

  @Description("config.mongo.replication.role")
  @NotNull
  @JsonProperty(required = true)
  @JsonDeserialize(using = RoleWithDefaultDeserializer.class)
  public EnumWithDefault<Role> getRole() {
    return role;
  }

  public void setRole(EnumWithDefault<Role> role) {
    this.role = role;
  }

  @Description("config.mongo.replication.syncSource")
  @JsonProperty(required = true)
  public StringWithDefault getSyncSource() {
    return syncSource;
  }

  public void setSyncSource(StringWithDefault syncSource) {
    this.syncSource = syncSource;
  }

  @Description("config.mongo.replication.ssl")
  @NotNull
  @JsonProperty(required = true)
  public Ssl getSsl() {
    return ssl;
  }

  public void setSsl(Ssl ssl) {
    this.ssl = ssl;
  }

  @Description("config.mongo.replication.auth")
  @NotNull
  @JsonProperty(required = true)
  public Auth getAuth() {
    return auth;
  }

  public void setAuth(Auth auth) {
    this.auth = auth;
  }
}
