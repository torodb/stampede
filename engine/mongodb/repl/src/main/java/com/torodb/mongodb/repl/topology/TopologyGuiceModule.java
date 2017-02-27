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

package com.torodb.mongodb.repl.topology;

import com.eightkdata.mongowp.client.core.MongoClientFactory;
import com.google.common.net.HostAndPort;
import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.guice.EssentialToDefaultModule;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.guice.ReplSetName;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;

import javax.inject.Singleton;

public class TopologyGuiceModule extends PrivateModule {

  private final TopologyBundleConfig config;

  public TopologyGuiceModule(TopologyBundleConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(SyncSourceProvider.class);
    expose(TopologyService.class);

    install(new EssentialToDefaultModule());

    bindConfig();

    bind(HeartbeatNetworkHandler.class)
        .to(MongoClientHeartbeatNetworkHandler.class)
        .in(Singleton.class);

    bind(SyncSourceProvider.class)
        .to(RetrierTopologySyncSourceProvider.class)
        .in(Singleton.class);

    bind(TopologyErrorHandler.class)
        .to(DefaultTopologyErrorHandler.class)
        .in(Singleton.class);

    bind(SyncSourceRetrier.class)
        .in(Singleton.class);

    bind(TopologyHeartbeatHandler.class)
        .in(Singleton.class);

    bind(TopologySyncSourceProvider.class)
        .in(Singleton.class);
  }

  @Provides
  @Topology
  Supervisor getTopologySupervisor(@MongoDbRepl Supervisor replSupervisor) {
    return replSupervisor;
  }

  @Provides
  @Singleton
  @Exposed
  public TopologyService createTopologyService(ThreadFactory threadFactory,
      TopologyHeartbeatHandler heartbeatHandler, TopologyExecutor executor,
      Clock clock) {
    return new TopologyService(heartbeatHandler, threadFactory, executor, clock);
  }

  @Provides
  @Singleton
  TopologyExecutor createTopologyExecutor(
      ConcurrentToolsFactory concurrentToolsFactory) {
    //TODO: Being able to configure max sync source lag and replication delay
    return new TopologyExecutor(concurrentToolsFactory, Duration.ofMinutes(1),
        Duration.ZERO);
  }

  private void bindConfig() {
    bind(HostAndPort.class)
        .annotatedWith(RemoteSeed.class)
        .toInstance(config.getSeed());

    bind(String.class)
        .annotatedWith(ReplSetName.class)
        .toInstance(config.getReplSetName());

    bind(MongoClientFactory.class)
        .toInstance(config.getClientFactory());

    bind(Supervisor.class)
        .annotatedWith(MongoDbRepl.class)
        .toInstance(config.getSupervisor());
  }
}
