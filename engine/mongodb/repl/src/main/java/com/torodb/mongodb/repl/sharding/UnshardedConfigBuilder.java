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

import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;


/**
 *
 */
class UnshardedConfigBuilder extends MongoDbShardingConfigBuilder {

  private MongoDbShardingConfig.ShardConfig shardConfig;

  protected UnshardedConfigBuilder(BundleConfig generalConfig) {
    super(generalConfig);
  }

  protected UnshardedConfigBuilder(Injector essentialInjector, Supervisor supervisor) {
    super(essentialInjector, supervisor);
  }

  @Override
  public MongoDbShardingConfigBuilder addShard(MongoDbShardingConfig.ShardConfig shardConfig) {
    if (this.shardConfig != null) {
      throw new IllegalArgumentException("A shard with name " + shardConfig.getShardId() + " has "
          + "been already added and this builder only supports a single shard.");
    }
    this.shardConfig = shardConfig;
    return this;
  }

  @Override
  protected MongoDbShardingConfig build(TorodBundle torodBundle, ReplicationFilters userReplFilter,
      LoggerFactory lifecycleLoggerFactory, BundleConfig generalConfig) {
    if (shardConfig == null) {
      throw new IllegalArgumentException("At least one shard is required");
    }

    return new MongoDbShardingConfig(
        torodBundle,
        shardConfig,
        userReplFilter,
        lifecycleLoggerFactory,
        generalConfig
    );
  }

}
