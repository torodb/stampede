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
import com.torodb.packaging.config.model.protocol.ProtocolListenerConfig;
import com.torodb.packaging.config.validation.Host;
import com.torodb.packaging.config.validation.Port;

import javax.validation.constraints.NotNull;
import javax.xml.bind.annotation.XmlType;

@XmlType
@JsonPropertyOrder({"bindIp", "port"})
public class Net implements ProtocolListenerConfig {

  @Description("config.mongo.net.bindIp")
  @NotNull
  @Host
  @JsonProperty(required = true)
  private String bindIp = "localhost";
  @Description("config.mongo.net.port")
  @NotNull
  @Port
  @JsonProperty(required = true)
  private Integer port = 27018;

  @Override
  public String getBindIp() {
    return bindIp;
  }

  public void setBindIp(String bindIp) {
    this.bindIp = bindIp;
  }

  @Override
  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }
}
