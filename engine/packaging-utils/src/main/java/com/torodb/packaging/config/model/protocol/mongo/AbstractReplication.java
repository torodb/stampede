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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.validation.RequiredParametersForAuthentication;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"replSetName", "syncSource", "role", "ssl", "auth", "include", "exclude"})
public abstract class AbstractReplication {

  private String replSetName;
  private Role role = Role.HIDDEN_SLAVE;
  private String syncSource;
  private Ssl ssl = new Ssl();
  private Auth auth = new Auth();
  private FilterList include;
  private FilterList exclude;

  @Description("config.mongo.replication.replSetName")
  @NotEmpty
  @JsonProperty(required = true)
  public String getReplSetName() {
    return replSetName;
  }

  public void setReplSetName(String replSetName) {
    this.replSetName = replSetName;
  }

  @Description("config.mongo.replication.role")
  @NotNull
  @JsonProperty(required = true)
  public Role getRole() {
    return role;
  }

  public void setRole(Role role) {
    this.role = role;
  }

  @Description("config.mongo.replication.syncSource")
  @NotNull
  @JsonProperty(required = true)
  public String getSyncSource() {
    return syncSource;
  }

  public void setSyncSource(String syncSource) {
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
  @RequiredParametersForAuthentication
  public Auth getAuth() {
    return auth;
  }

  public void setAuth(Auth auth) {
    this.auth = auth;
  }

  @Description("config.mongo.replication.include")
  @JsonProperty(required = true)
  public FilterList getInclude() {
    return include;
  }

  public void setInclude(FilterList include) {
    this.include = include;
  }

  @Description("config.mongo.replication.exclude")
  @JsonProperty(required = true)
  public FilterList getExclude() {
    return exclude;
  }

  public void setExclude(FilterList exclude) {
    this.exclude = exclude;
  }
}
