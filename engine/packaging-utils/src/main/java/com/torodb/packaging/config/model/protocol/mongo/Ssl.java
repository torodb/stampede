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

import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"enabled", "allowInvalidHostnames", "FIPSMode", "CAFile", "trustStoreFile",
    "trustStorePassword", "keyStoreFile", "keyStorePassword", "keyPassword"})
public class Ssl {

  @Description("config.mongo.replication.ssl.enabled")
  @NotNull
  @JsonProperty(required = true)
  private Boolean enabled = false;
  @Description("config.mongo.replication.ssl.allowInvalidHostnames")
  @NotNull
  @JsonProperty(required = true)
  private Boolean allowInvalidHostnames = false;
  @Description("config.mongo.replication.ssl.fipsMode")
  @NotNull
  @JsonProperty(required = true)
  private Boolean fipsMode = false;
  @Description("config.mongo.replication.ssl.caFile")
  @JsonProperty(required = true)
  private String caFile;
  @Description("config.mongo.replication.ssl.trustStoreFile")
  @JsonProperty(required = true)
  private String trustStoreFile;
  @Description("config.mongo.replication.ssl.trustStorePassword")
  @JsonProperty(required = true)
  private String trustStorePassword;
  @Description("config.mongo.replication.ssl.keyStoreFile")
  @JsonProperty(required = true)
  private String keyStoreFile;
  @Description("config.mongo.replication.ssl.keyStorePassword")
  @JsonProperty(required = true)
  private String keyStorePassword;
  @Description("config.mongo.replication.ssl.keyPassword")
  @JsonProperty(required = true)
  private String keyPassword;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Boolean getAllowInvalidHostnames() {
    return allowInvalidHostnames;
  }

  public void setAllowInvalidHostnames(Boolean allowInvalidHostnames) {
    this.allowInvalidHostnames = allowInvalidHostnames;
  }

  public Boolean getFipsMode() {
    return fipsMode;
  }

  public void setFipsMode(Boolean fipsMode) {
    this.fipsMode = fipsMode;
  }

  public String getCaFile() {
    return caFile;
  }

  public void setCaFile(String cAFile) {
    caFile = cAFile;
  }

  public String getTrustStoreFile() {
    return trustStoreFile;
  }

  public void setTrustStoreFile(String trustStoreFile) {
    this.trustStoreFile = trustStoreFile;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  public String getKeyStoreFile() {
    return keyStoreFile;
  }

  public void setKeyStoreFile(String keyStoreFile) {
    this.keyStoreFile = keyStoreFile;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  public String getKeyPassword() {
    return keyPassword;
  }

  public void setKeyPassword(String keyPassword) {
    this.keyPassword = keyPassword;
  }
}
