/*
 * ToroDB Stampede
 * Copyright © 2016 8Kdata Technology (www.8kdata.com)
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
package com.torodb.stampede.config.model.mongo.replication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.common.EnumWithDefault;
import com.torodb.packaging.config.model.common.ListOfStringWithDefault;
import com.torodb.packaging.config.model.common.StringWithDefault;
import com.torodb.packaging.config.model.protocol.mongo.AbstractShardReplication;
import com.torodb.packaging.config.model.protocol.mongo.Auth;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.model.protocol.mongo.Ssl;
import com.torodb.packaging.config.validation.NotEmptyListOfSrtingWithDefault;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"replSetName", "syncSource", "ssl", "auth"})
public class ShardReplication extends AbstractShardReplication {

  @Description("config.mongo.replication.replSetName")
  @JsonProperty(required = true)
  public StringWithDefault getReplSetName() {
    return super.getReplSetName();
  }

  @JsonIgnore
  public EnumWithDefault<Role> getRole() {
    return super.getRole();
  }

  @Description("config.mongo.replication.syncSource")
  @NotEmptyListOfSrtingWithDefault
  @JsonProperty(required = false)
  public ListOfStringWithDefault getSyncSource() {
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
  public Auth getAuth() {
    return super.getAuth();
  }
}
