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
import com.torodb.packaging.config.jackson.AuthModeWithDefaultDeserializer;
import com.torodb.packaging.config.model.common.EnumWithDefault;
import com.torodb.packaging.config.model.common.StringWithDefault;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"mode", "user", "source"})
public class Auth {

  private EnumWithDefault<AuthMode> mode = 
      EnumWithDefault.withDefault(AuthMode.disabled);
  private StringWithDefault user = StringWithDefault.withDefault(null);
  private StringWithDefault source = StringWithDefault.withDefault(null);
  private String password;
  
  @Description("config.mongo.replication.auth.mode")
  @NotNull
  @JsonProperty(required = true)
  @JsonDeserialize(using = AuthModeWithDefaultDeserializer.class)
  public EnumWithDefault<AuthMode> getMode() {
    return mode;
  }

  public void setMode(EnumWithDefault<AuthMode> mode) {
    this.mode = mode;
  }

  @Description("config.mongo.replication.auth.user")
  @JsonProperty(required = true)
  public StringWithDefault getUser() {
    return user;
  }

  public void setUser(StringWithDefault user) {
    this.user = user;
  }

  @Description("config.mongo.replication.auth.source")
  @JsonProperty(required = true)
  public StringWithDefault getSource() {
    return source;
  }

  public void setSource(StringWithDefault source) {
    this.source = source;
  }

  @JsonIgnore
  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
