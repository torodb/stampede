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

package com.torodb.stampede.config.model.backend;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Description("config.pool")
@JsonPropertyOrder({"connectionPoolTimeout", "connectionPoolSize"})
public class Pool implements ConnectionPoolConfig {

  @Description("config.generic.connectionPoolTimeout")
  @NotNull
  @JsonProperty(required = true)
  private Long connectionPoolTimeout = 10L * 1000;
  @Description("config.generic.connectionPoolSize")
  @NotNull
  @Min(3)
  @JsonProperty(required = true)
  private Integer connectionPoolSize = 30;
  @NotNull
  @Min(1)
  @JsonIgnore
  private Integer reservedReadPoolSize = 1;

  @Override
  public Long getConnectionPoolTimeout() {
    return connectionPoolTimeout;
  }

  public void setConnectionPoolTimeout(Long connectionPoolTimeout) {
    this.connectionPoolTimeout = connectionPoolTimeout;
  }

  @Override
  public Integer getConnectionPoolSize() {
    return connectionPoolSize;
  }

  public void setConnectionPoolSize(Integer connectionPoolSize) {
    this.connectionPoolSize = connectionPoolSize;
  }

  @Override
  public Integer getReservedReadPoolSize() {
    return reservedReadPoolSize;
  }

  public void setReservedReadPoolSize(Integer reserverdReadPoolSize) {
    this.reservedReadPoolSize = reserverdReadPoolSize;
  }

}
