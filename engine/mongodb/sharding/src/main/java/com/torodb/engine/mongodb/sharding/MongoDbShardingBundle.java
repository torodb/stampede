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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MongoDbShardingBundle extends AbstractBundle<MongoDbShardingExtInt> {
  
  private final MongoDbShardingConfig config;
  private final List<ShardBundle> shards;

  public MongoDbShardingBundle(MongoDbShardingConfig bundleConfig) {
    super(bundleConfig);
    this.config = bundleConfig;

    this.shards = createShards(bundleConfig);
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    for (ShardBundle shard : shards) {
      shard.startAsync();
    }
    for (ShardBundle shard : shards) {
      shard.awaitRunning();
    }
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    for (ShardBundle shard : shards) {
      shard.stopAsync();
    }
    for (ShardBundle shard : shards) {
      shard.awaitTerminated();
    }
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.singleton(config.getTorodBundle());
  }

  @Override
  public MongoDbShardingExtInt getExternalInterface() {
    return new MongoDbShardingExtInt();
  }

  private static List<ShardBundle> createShards(MongoDbShardingConfig generalConf) {
    List<ShardBundle> result = new ArrayList<>(generalConf.getShardConfigs().size());

    for (MongoDbShardingConfig.ShardConfig shardConfig : generalConf.getShardConfigs()) {
      result.add(new ShardBundle(toShardBundleConfig(generalConf, shardConfig)));
    }

    return result;
  }

  private static ShardBundleConfig toShardBundleConfig(MongoDbShardingConfig generalConf,
      MongoDbShardingConfig.ShardConfig shardConfig) {

    return new ShardBundleConfig(
        shardConfig.getShardId(),
        generalConf.getTorodBundle(),
        shardConfig.getClientConfig(),
        shardConfig.getReplSetName(),
        generalConf.getUserReplFilter(),
        shardConfig.getConsistencyHandler(),
        generalConf.getEssentialInjector(),
        generalConf.getSupervisor()
    );

  }

}
