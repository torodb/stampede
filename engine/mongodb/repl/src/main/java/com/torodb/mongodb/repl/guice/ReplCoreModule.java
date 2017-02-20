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

package com.torodb.mongodb.repl.guice;

import com.eightkdata.mongowp.client.core.CachedMongoClientFactory;
import com.eightkdata.mongowp.client.core.GuavaCachedMongoClientFactory;
import com.eightkdata.mongowp.client.core.MongoClientFactory;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapperFactory;
import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.ReplCoreConfig;
import com.torodb.mongodb.repl.ReplMetrics;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.mongodb.repl.impl.MongoOplogReaderProvider;

import java.time.Duration;

import javax.inject.Singleton;

public class ReplCoreModule extends PrivateModule {

  private final ReplCoreConfig config;

  public ReplCoreModule(ReplCoreConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(OplogManager.class);
    expose(CachedMongoClientFactory.class);
    expose(OplogReaderProvider.class);
    expose(ReplMetrics.class);
    
    bind(Supervisor.class)
        .annotatedWith(MongoDbRepl.class)
        .toInstance(config.getSupervisor());

    bind(ReplicationFilters.class)
        .toInstance(config.getReplicationFilters());

    bind(MongodServer.class)
        .toInstance(config.getMongoDbCoreBundle().getExternalInterface().getMongodServer());

    bind(OplogManager.class)
        .in(Singleton.class);
    bind(ReplMetrics.class)
        .in(Singleton.class);

    bind(MongoClientConfiguration.class)
        .toInstance(config.getMongoClientConfig());
    bind(MongoClientWrapperFactory.class)
        .in(Singleton.class);
    bind(MongoClientFactory.class)
        .to(CachedMongoClientFactory.class)
        .in(Singleton.class);

    bind(OplogReaderProvider.class).to(MongoOplogReaderProvider.class).asEagerSingleton();
  }

  @Provides
  @Singleton
  @Exposed
  CachedMongoClientFactory getMongoClientFactory(MongoClientWrapperFactory wrapperFactory) {
    return new GuavaCachedMongoClientFactory(wrapperFactory, Duration.ofMinutes(10));
  }

}
