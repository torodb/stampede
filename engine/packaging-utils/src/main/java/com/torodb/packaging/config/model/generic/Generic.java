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

package com.torodb.packaging.config.model.generic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.core.metrics.MetricsConfig;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.backend.ConnectionPoolConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Description("config.generic")
@JsonPropertyOrder({"logLevel", "logFile", "connectionPoolSize", "reservedReadPoolSize"})
public class Generic implements ConnectionPoolConfig, MetricsConfig {

  @Description("config.generic.logLevel")
  @JsonProperty(required = false)
  private LogLevel logLevel;
  @Description("config.generic.logPackages")
  private LogPackages logPackages;
  @Description("config.generic.logFile")
  private String logFile;
  @Description("config.generic.log4j2File")
  private String log4j2File;
  @Description("config.generic.connectionPoolTimeout")
  @NotNull
  @JsonProperty(required = true)
  private Long connectionPoolTimeout = 10L * 1000;
  @Description("config.generic.connectionPoolSize")
  @NotNull
  @Min(3)
  @JsonProperty(required = true)
  private Integer connectionPoolSize = 30;
  @Description("config.generic.reservedReadPoolSize")
  @NotNull
  @Min(1)
  @JsonProperty(required = true)
  private Integer reservedReadPoolSize = 10;
  @Description("config.generic.metricsEnabled")
  private Boolean metricsEnabled = false;

  public LogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
  }

  public LogPackages getLogPackages() {
    return logPackages;
  }

  public void setLogPackages(LogPackages logPackages) {
    this.logPackages = logPackages;
  }

  public String getLogFile() {
    return logFile;
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
  }

  public String getLog4j2File() {
    return log4j2File;
  }

  public void setLog4j2File(String log4j2File) {
    this.log4j2File = log4j2File;
  }

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

  @Override
  public Boolean getMetricsEnabled() {
    return metricsEnabled;
  }

  public void setMetricsEnabled(Boolean metricsEnabled) {
    if (metricsEnabled != null) {
      this.metricsEnabled = metricsEnabled;
    }
  }

}
