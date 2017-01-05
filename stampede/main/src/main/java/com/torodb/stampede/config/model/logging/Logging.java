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

package com.torodb.stampede.config.model.logging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.packaging.config.annotation.Description;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.generic.LogPackages;

@Description("config.logging")
@JsonPropertyOrder({"logLevel", "logPackages", "logFile", "log4j2File"})
public class Logging {

  @Description("config.generic.logLevel")
  @JsonProperty(required = false)
  private LogLevel level;
  @Description("config.generic.logPackages")
  private LogPackages packages;
  @Description("config.generic.logFile")
  private String file;
  @Description("config.generic.log4j2File")
  private String log4j2File;

  public LogLevel getLevel() {
    return level;
  }

  public void setLevel(LogLevel logLevel) {
    this.level = logLevel;
  }

  public LogPackages getPackages() {
    return packages;
  }

  public void setPackages(LogPackages logPackages) {
    this.packages = logPackages;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String logFile) {
    this.file = logFile;
  }

  public String getLog4j2File() {
    return log4j2File;
  }

  public void setLog4j2File(String log4j2File) {
    this.log4j2File = log4j2File;
  }

}
