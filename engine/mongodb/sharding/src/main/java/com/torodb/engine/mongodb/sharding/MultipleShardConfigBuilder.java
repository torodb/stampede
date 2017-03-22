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

import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class MultipleShardConfigBuilder extends MongoDbShardingConfigBuilder {

  private final Map<String, MongoDbShardingConfig.ShardConfig> shardConfigs = new HashMap<>();

  protected MultipleShardConfigBuilder(BundleConfig generalConfig) {
    super(generalConfig);
  }

  protected MultipleShardConfigBuilder(Injector essentialInjector, Supervisor supervisor) {
    super(essentialInjector, supervisor);
  }
  
  @Override
  public MongoDbShardingConfigBuilder addShard(MongoDbShardingConfig.ShardConfig config) {
    if (shardConfigs.containsKey(config.getShardId())) {
      throw new IllegalArgumentException("The shard " + config.getShardId() + " has been already"
          + "added");
    }
    shardConfigs.put(config.getShardId(), config);
    return this;
  }

  @Override
  protected MongoDbShardingConfig build(TorodBundle torodBundle, ReplicationFilters userReplFilter,
      LoggerFactory lifecycleLoggerFactory, BundleConfig generalConfig) {
    if (shardConfigs.isEmpty()) {
      throw new IllegalArgumentException("At least one shard is required");
    }

    return new MongoDbShardingConfig(
        torodBundle,
        new ArrayList<>(shardConfigs.values()),
        userReplFilter,
        lifecycleLoggerFactory,
        generalConfig
    );
  }

}
