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

package com.torodb.config.model.protocol.mongo;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlType;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.torodb.config.annotation.Description;
import com.torodb.config.validation.Host;
import com.torodb.config.validation.Port;

@XmlType
@JsonPropertyOrder({ "bindIp", "port" })
public class Net {
	@Description("config.protocol.mongo.net.bindIp")
	@NotNull
	@Host
	@JsonProperty(required=true)
	private String bindIp = "localhost";
	@Description("config.protocol.mongo.net.port")
	@NotNull
	@Port
	@JsonProperty(required=true)
	private Integer port = 27018;

	public String getBindIp() {
		return bindIp;
	}

	public void setBindIp(String bindIp) {
		this.bindIp = bindIp;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
}
