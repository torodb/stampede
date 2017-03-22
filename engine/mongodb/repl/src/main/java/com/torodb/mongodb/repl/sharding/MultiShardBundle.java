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

package com.torodb.mongodb.repl.sharding;

import com.google.common.util.concurrent.Service;
import com.google.inject.Key;
import com.torodb.core.guice.Essential;
import com.torodb.core.logging.ComponentLoggerFactory;
import com.torodb.core.metrics.ToroMetricRegistry;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.repl.MongoDbReplBundle;
import com.torodb.mongodb.repl.MongoDbReplConfig;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;
import com.torodb.mongodb.repl.sharding.isolation.db.DbIsolatedTorodBundle;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class MultiShardBundle extends ShardBundle {

  private final Logger logger;
  private final String shardId;
  private final TorodBundle actualTorodBundle;
  private final TorodBundle torodBundle;
  private final MongoDbCoreBundle coreBundle;
  private final MongoDbReplBundle replBundle;

  MultiShardBundle(ShardBundleConfig config) {
    super(config);
    this.logger = config.getLifecycleLoggingFactory().apply(this.getClass());

    actualTorodBundle = config.getTorodBundle();
    TorodServer actualTorod = actualTorodBundle.getExternalInterface().getTorodServer();

    shardId = config.getShardId();
    Optional<ToroMetricRegistry> shardMetricRegistry = createToroMetricRegistry(config);
    ComponentLoggerFactory mongodLf = new ComponentLoggerFactory("MONGOD-" + shardId);

    torodBundle = new DbIsolatedTorodBundle(shardId, actualTorod, mongodLf, config);
    coreBundle = new MongoDbCoreBundle(
        MongoDbCoreConfig.simpleNonServerConfig(
            torodBundle,
            mongodLf,
            shardMetricRegistry,
            config
        )
    );
    replBundle = new MongoDbReplBundle(createReplConfig(config, shardMetricRegistry, coreBundle));
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    logger.info("Starting replication shard {}", shardId);

    torodBundle.startAsync();
    torodBundle.awaitRunning();

    coreBundle.startAsync();
    coreBundle.awaitRunning();

    replBundle.startAsync();
    replBundle.awaitRunning();

    logger.info("Replication shard {} has been started", shardId);
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    logger.info("Shutting down replication shard {}", shardId);

    replBundle.stopAsync();
    replBundle.awaitTerminated();

    coreBundle.stopAsync();
    coreBundle.awaitTerminated();

    torodBundle.stopAsync();
    torodBundle.awaitTerminated();

    logger.info("Replication shard {} has been shutted down", shardId);
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  private static MongoDbReplConfig createReplConfig(
      ShardBundleConfig config, Optional<ToroMetricRegistry> shardMetricRegistry,
      MongoDbCoreBundle coreBundle) {
    return new MongoDbReplConfigBuilder(config)
        .setConsistencyHandler(config.getConsistencyHandler())
        .setCoreBundle(coreBundle)
        .setMongoClientConfiguration(config.getClientConfig())
        .setReplSetName(config.getReplSetName())
        .setReplicationFilters(config.getUserReplFilter())
        .setMetricRegistry(shardMetricRegistry)
        .setLoggerFactory(new ComponentLoggerFactory("REPL-" + config.getShardId()))
        .build();
  }

  private static Optional<ToroMetricRegistry> createToroMetricRegistry(ShardBundleConfig config) {
    ToroMetricRegistry parentRegistry = config.getEssentialInjector().getInstance(
        Key.get(ToroMetricRegistry.class, Essential.class)
    );

    return Optional.of(parentRegistry
        .createSubRegistry("replication")
        .createSubRegistry("shard", config.getShardId())
    );
  }

}
