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

package com.torodb.packaging.guice;

import com.eightkdata.mongowp.server.MongoServerConfig;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.torodb.core.BuildProperties;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.packaging.DefaultBuildProperties;
import com.torodb.packaging.config.model.protocol.ProtocolListenerConfig;

public class MongoServerModule extends AbstractModule {

  private final ProtocolListenerConfig protocolListenerConfig;

  public MongoServerModule(
      ProtocolListenerConfig protocolListenerConfig) {
    this.protocolListenerConfig = protocolListenerConfig;
  }

  @Override
  protected void configure() {
    bind(BuildProperties.class).to(DefaultBuildProperties.class).asEagerSingleton();

    bind(MongoServerConfig.class)
        .toInstance(() -> protocolListenerConfig.getPort());
  }

  @Provides
  public MongodServerConfig createMongodServerConfig() {
    return new MongodServerConfig(HostAndPort.fromParts(
        protocolListenerConfig.getBindIp(),
        protocolListenerConfig.getPort()
    ));
  }
}
