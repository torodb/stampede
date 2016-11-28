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
import com.torodb.packaging.config.annotation.Description;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"mode", "user", "source"})
public class Auth {

  @Description("config.mongo.replication.auth.mode")
  @NotNull
  @JsonProperty(required = true)
  private AuthMode mode = AuthMode.disabled;
  @Description("config.mongo.replication.auth.user")
  @JsonProperty(required = true)
  private String user;
  @Description("config.mongo.replication.auth.source")
  @JsonProperty(required = true)
  private String source;
  @JsonIgnore
  private String password;

  public AuthMode getMode() {
    return mode;
  }

  public void setMode(AuthMode mode) {
    this.mode = mode;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
