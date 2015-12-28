/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.config.model.generic;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.annotation.Description;

@Description("config.generic")
@JsonPropertyOrder({ 
	"logLevel", 
	"logFile", 
	"connectionPoolSize", 
	"reservedReadPoolSize" 
})
public class Generic {

	@Description("config.generic.logLevel")
	@NotNull
	@JsonProperty(required=true)
	private LogLevel logLevel = LogLevel.INFO;
	@Description("config.generic.logPackages")
	private LogPackages logPackages;
	@Description("config.generic.logFile")
	private String logFile;
	@Description("config.generic.logbackFile")
	private String logbackFile;
	@Description("config.generic.connectionPoolSize")
	@NotNull
	@Min(3)
	@JsonProperty(required=true)
	private Integer connectionPoolSize = 30;
	@Description("config.generic.reservedReadPoolSize")
	@NotNull
	@Min(1)
	@JsonProperty(required=true)
	private Integer reservedReadPoolSize = 10;

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

	public String getLogbackFile() {
		return logbackFile;
	}

	public void setLogbackFile(String logbackFile) {
		this.logbackFile = logbackFile;
	}

	public Integer getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public void setConnectionPoolSize(Integer connectionPoolSize) {
		this.connectionPoolSize = connectionPoolSize;
	}

	public Integer getReservedReadPoolSize() {
		return reservedReadPoolSize;
	}

	public void setReservedReadPoolSize(Integer reserverdReadPoolSize) {
		this.reservedReadPoolSize = reserverdReadPoolSize;
	}
}
