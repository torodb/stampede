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

package com.torodb.stampede.config.model.replication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.CursorConfig;
import com.torodb.packaging.config.model.protocol.mongo.AbstractReplication;
import com.torodb.packaging.config.model.protocol.mongo.Auth;
import com.torodb.packaging.config.model.protocol.mongo.FilterList;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.model.protocol.mongo.Ssl;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.config.validation.RequiredParametersForAuthentication;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"replSetName", "syncSource", "ssl", "auth", "include", "exclude",
    "mongopassFile"})
public class Replication extends AbstractReplication implements CursorConfig {

  private Long cursorTimeout = 10L * 60 * 1000;
  private String mongopassFile = ConfigUtils.getUserHomeFilePath(".mongopass");

  public Replication() {
    setSyncSource("localhost:27017");
    setReplSetName("rs1");
  }

  @Override
  @JsonIgnore
  public Long getCursorTimeout() {
    return cursorTimeout;
  }

  public void setCursorTimeout(Long cursorTimeout) {
    this.cursorTimeout = cursorTimeout;
  }

  @Description("config.mongo.mongopassFile")
  @JsonProperty(required = true)
  public String getMongopassFile() {
    return mongopassFile;
  }

  public void setMongopassFile(String mongopassFile) {
    this.mongopassFile = mongopassFile;
  }

  @Description("config.mongo.replication.replSetName")
  @NotEmpty
  @JsonProperty(required = true)
  public String getReplSetName() {
    return super.getReplSetName();
  }

  @JsonIgnore
  public Role getRole() {
    return super.getRole();
  }

  @Description("config.mongo.replication.syncSource")
  @NotNull
  @JsonProperty(required = true)
  public String getSyncSource() {
    return super.getSyncSource();
  }

  @Description("config.mongo.replication.ssl")
  @NotNull
  @JsonProperty(required = true)
  public Ssl getSsl() {
    return super.getSsl();
  }

  @Description("config.mongo.replication.auth")
  @NotNull
  @JsonProperty(required = true)
  @RequiredParametersForAuthentication
  public Auth getAuth() {
    return super.getAuth();
  }

  @Description("config.mongo.replication.include")
  @JsonProperty(required = true)
  public FilterList getInclude() {
    return super.getInclude();
  }

  @Description("config.mongo.replication.exclude")
  @JsonProperty(required = true)
  public FilterList getExclude() {
    return super.getExclude();
  }
}
