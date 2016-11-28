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

package com.torodb.standalone;

import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.torodb.concurrent.guice.ConcurrentModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.metrics.guice.MetricsModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.packaging.DefaultBuildProperties;
import com.torodb.packaging.guice.BackendDerbyImplementationModule;
import com.torodb.packaging.guice.BackendMultiImplementationModule;
import com.torodb.packaging.guice.BackendPostgresImplementationModule;
import com.torodb.packaging.guice.ExecutorServicesModule;
import com.torodb.packaging.guice.PackagingModule;
import com.torodb.standalone.config.model.Config;

import java.time.Clock;

/**
 *
 */
class BootstrapModule extends AbstractModule {

  private final Config config;
  private final Clock clock;

  public BootstrapModule(Config config, Clock clock) {
    this.config = config;
    this.clock = clock;
  }

  @Override
  protected void configure() {
    binder().requireExplicitBindings();
    install(new PackagingModule(clock));
    install(new CoreModule());
    install(new ExecutorServicesModule());
    install(new ConcurrentModule());
    install(new MetainfModule());
    install(new MetricsModule(config.getGeneric()));

    install(new BackendMultiImplementationModule(
        config.getProtocol().getMongo(),
        config.getGeneric(),
        config.getBackend().getBackendImplementation(),
        new BackendPostgresImplementationModule(),
        new BackendDerbyImplementationModule()
    ));

    bind(Config.class)
        .toInstance(config);
    bind(MongodServerConfig.class)
        .toInstance(new MongodServerConfig(HostAndPort.fromParts("localhost", 27017)));
    bind(BuildProperties.class)
        .to(DefaultBuildProperties.class)
        .asEagerSingleton();
  }

}
