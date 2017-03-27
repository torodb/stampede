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
import com.torodb.packaging.config.model.common.BooleanWithDefault;
import com.torodb.packaging.config.model.common.StringWithDefault;

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"enabled", "allowInvalidHostnames", "FIPSMode", "CAFile", "trustStoreFile",
    "trustStorePassword", "keyStoreFile", "keyStorePassword", "keyPassword"})
public class Ssl {

  private BooleanWithDefault enabled = BooleanWithDefault.withDefault(false);
  private BooleanWithDefault allowInvalidHostnames = BooleanWithDefault.withDefault(false);
  private BooleanWithDefault fipsMode = BooleanWithDefault.withDefault(false);
  private StringWithDefault caFile = StringWithDefault.withDefault(null);
  private StringWithDefault trustStoreFile = StringWithDefault.withDefault(null);
  private StringWithDefault trustStorePassword = StringWithDefault.withDefault(null);
  private StringWithDefault keyStoreFile = StringWithDefault.withDefault(null);
  private StringWithDefault keyStorePassword = StringWithDefault.withDefault(null);
  private StringWithDefault keyPassword = StringWithDefault.withDefault(null);
  
  @Description("config.mongo.replication.ssl.enabled")
  @NotNull
  @JsonProperty(required = true)
  public BooleanWithDefault getEnabled() {
    return enabled;
  }

  public void setEnabled(BooleanWithDefault enabled) {
    this.enabled = enabled;
  }

  @Description("config.mongo.replication.ssl.allowInvalidHostnames")
  @NotNull
  @JsonProperty(required = true)
  public BooleanWithDefault getAllowInvalidHostnames() {
    return allowInvalidHostnames;
  }

  public void setAllowInvalidHostnames(BooleanWithDefault allowInvalidHostnames) {
    this.allowInvalidHostnames = allowInvalidHostnames;
  }

  @Description("config.mongo.replication.ssl.fipsMode")
  @NotNull
  @JsonProperty(required = true)
  public BooleanWithDefault getFipsMode() {
    return fipsMode;
  }

  public void setFipsMode(BooleanWithDefault fipsMode) {
    this.fipsMode = fipsMode;
  }

  @Description("config.mongo.replication.ssl.caFile")
  @JsonProperty(required = true)
  public StringWithDefault getCaFile() {
    return caFile;
  }

  public void setCaFile(StringWithDefault caFile) {
    this.caFile = caFile;
  }

  @Description("config.mongo.replication.ssl.trustStoreFile")
  @JsonProperty(required = true)
  public StringWithDefault getTrustStoreFile() {
    return trustStoreFile;
  }

  public void setTrustStoreFile(StringWithDefault trustStoreFile) {
    this.trustStoreFile = trustStoreFile;
  }

  @Description("config.mongo.replication.ssl.trustStorePassword")
  @JsonProperty(required = true)
  public StringWithDefault getTrustStorePassword() {
    return trustStorePassword;
  }

  public void setTrustStorePassword(StringWithDefault trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  @Description("config.mongo.replication.ssl.keyStoreFile")
  @JsonProperty(required = true)
  public StringWithDefault getKeyStoreFile() {
    return keyStoreFile;
  }

  public void setKeyStoreFile(StringWithDefault keyStoreFile) {
    this.keyStoreFile = keyStoreFile;
  }

  @Description("config.mongo.replication.ssl.keyStorePassword")
  @JsonProperty(required = true)
  public StringWithDefault getKeyStorePassword() {
    return keyStorePassword;
  }

  public void setKeyStorePassword(StringWithDefault keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  @Description("config.mongo.replication.ssl.keyPassword")
  @JsonProperty(required = true)
  public StringWithDefault getKeyPassword() {
    return keyPassword;
  }

  public void setKeyPassword(StringWithDefault keyPassword) {
    this.keyPassword = keyPassword;
  }
}
