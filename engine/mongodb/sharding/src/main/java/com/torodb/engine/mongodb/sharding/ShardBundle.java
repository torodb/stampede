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

package com.torodb.engine.mongodb.sharding;

import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.engine.mongodb.sharding.isolation.db.DbIsolatedTorodBundle;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.repl.MongoDbReplBundle;
import com.torodb.mongodb.repl.MongoDbReplConfig;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;

public class ShardBundle extends AbstractBundle<ShardBundle> {

  private static final Logger LOGGER = LogManager.getLogger(ShardBundle.class);
  private final String shardId;
  private final TorodBundle torodBundle;
  private final MongoDbCoreBundle coreBundle;
  private final MongoDbReplBundle replBundle;

  ShardBundle(ShardBundleConfig config) {
    super(config);

    TorodServer realTorod = config.getTorodBundle().getExternalInterface().getTorodServer();
    shardId = config.getShardId();
    torodBundle = new DbIsolatedTorodBundle(shardId, realTorod, config);
    coreBundle = new MongoDbCoreBundle(
        MongoDbCoreConfig.simpleNonServerConfig(torodBundle, config)
    );
    replBundle = new MongoDbReplBundle(createReplConfig(config, coreBundle));
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.info("Starting replication shard {}", shardId);

    torodBundle.startAsync();
    torodBundle.awaitRunning();

    coreBundle.startAsync();
    coreBundle.awaitRunning();

    replBundle.startAsync();
    replBundle.awaitRunning();

    LOGGER.info("Replication shard {} has been started", shardId);
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    LOGGER.info("Shutting down replication shard {}", shardId);

    replBundle.stopAsync();
    replBundle.awaitTerminated();

    coreBundle.stopAsync();
    coreBundle.awaitTerminated();

    torodBundle.stopAsync();
    torodBundle.awaitTerminated();

    LOGGER.info("Replication shard {} has been shutted down", shardId);
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.emptyList();
  }

  @Override
  public ShardBundle getExternalInterface() {
    return this;
  }

  private static MongoDbReplConfig createReplConfig(
      ShardBundleConfig config, MongoDbCoreBundle coreBundle) {
    return new MongoDbReplConfigBuilder(config)
        .setConsistencyHandler(config.getConsistencyHandler())
        .setCoreBundle(coreBundle)
        .setMongoClientConfiguration(config.getClientConfig())
        .setReplSetName(config.getReplSetName())
        .setReplicationFilters(config.getUserReplFilter())
        .build();
  }

}
