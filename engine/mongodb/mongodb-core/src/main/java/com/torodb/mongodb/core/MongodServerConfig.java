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

package com.torodb.mongodb.core;

import com.eightkdata.mongowp.server.MongoServerConfig;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 *
 */
public class MongodServerConfig implements MongoServerConfig {

  private final HostAndPort hostAndPort;

  public MongodServerConfig(HostAndPort hostAndPort) {
    this.hostAndPort = hostAndPort;
    Preconditions.checkArgument(hostAndPort.hasPort(),
        "The host and port of a given mongod server must have a port");
  }

  public HostAndPort getHostAndPort() {
    return hostAndPort;
  }

  @Override
  public int getPort() {
    return hostAndPort.getPort();
  }

}
