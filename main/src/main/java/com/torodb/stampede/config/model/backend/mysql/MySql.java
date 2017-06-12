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
package com.torodb.stampede.config.model.backend.mysql;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.mysql.AbstractMySql;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.config.validation.ExistsAnyPassword;
import com.torodb.packaging.config.validation.Host;
import com.torodb.packaging.config.validation.Port;

import javax.validation.constraints.NotNull;

@Description("config.backend.mysql")
@JsonPropertyOrder({"host", "port", "database", "user", "password", "toropassFile",
    "applicationName", "includeForeignKeys", "ssl"})
@ExistsAnyPassword
public class MySql extends AbstractMySql {

  public MySql() {
    super(
        "localhost",
        3306,
        "torod",
        "torodb",
        null,
        ConfigUtils.getUserHomeFilePath(".toropass"),
        "toro",
        false, 
        false
    );
  }

  @Description("config.backend.mysql.host")
  @NotNull
  @Host
  @JsonProperty(required = true)
  @Override
  public String getHost() {
    return super.getHost();
  }

  @Description("config.backend.mysql.port")
  @NotNull
  @Port
  @JsonProperty(required = true)
  @Override
  public Integer getPort() {
    return super.getPort();
  }

  @Description("config.backend.mysql.database")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getDatabase() {
    return super.getDatabase();
  }

  @Description("config.backend.mysql.user")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getUser() {
    return super.getUser();
  }

  @JsonIgnore
  @Override
  public String getPassword() {
    return super.getPassword();
  }

  @Description("config.backend.mysql.toropassFile")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getToropassFile() {
    return super.getToropassFile();
  }

  @Description("config.backend.mysql.applicationName")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getApplicationName() {
    return super.getApplicationName();
  }

  @JsonIgnore
  @Override
  public Boolean getIncludeForeignKeys() {
    return super.getIncludeForeignKeys();
  }
  
  @Description("config.backend.mysql.ssl")
  @JsonProperty(required = false)
  @NotNull
  @Override
  public Boolean getSsl() {
    return super.getSsl();
  }
}
