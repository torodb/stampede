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

package com.torodb.stampede.config.model.backend.postgres;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.postgres.AbstractPostgres;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.config.validation.ExistsAnyPassword;
import com.torodb.packaging.config.validation.Host;
import com.torodb.packaging.config.validation.Port;

import javax.validation.constraints.NotNull;

@Description("config.backend.postgres")
@JsonPropertyOrder({"host", "port", "database", "user", "password", "toropassFile",
    "applicationName", "includeForeignKeys"})
@ExistsAnyPassword
public class Postgres extends AbstractPostgres {

  public Postgres() {
    super(
        "localhost",
        5432,
        "torod",
        "torodb",
        null,
        ConfigUtils.getUserHomeFilePath(".toropass"),
        "toro",
        false
    );
  }

  @Description("config.backend.postgres.host")
  @NotNull
  @Host
  @JsonProperty(required = true)
  @Override
  public String getHost() {
    return super.getHost();
  }

  @Description("config.backend.postgres.port")
  @NotNull
  @Port
  @JsonProperty(required = true)
  @Override
  public Integer getPort() {
    return super.getPort();
  }

  @Description("config.backend.postgres.database")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getDatabase() {
    return super.getDatabase();
  }

  @Description("config.backend.postgres.user")
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

  @Description("config.backend.postgres.toropassFile")
  @NotNull
  @JsonProperty(required = true)
  @Override
  public String getToropassFile() {
    return super.getToropassFile();
  }

  @Description("config.backend.postgres.applicationName")
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
}
