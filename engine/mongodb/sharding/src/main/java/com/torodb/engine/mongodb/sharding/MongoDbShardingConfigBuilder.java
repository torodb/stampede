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
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.supervision.Supervisor;
import com.torodb.engine.mongodb.sharding.MongoDbShardingConfig.ShardConfig;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MongoDbShardingConfigBuilder {

  private TorodBundle torodBundle;
  private final Map<String, MongoDbShardingConfig.ShardConfig> shardConfigs = new HashMap<>();
  private ReplicationFilters userReplFilter;
  private final BundleConfig generalConfig;

  public MongoDbShardingConfigBuilder(BundleConfig generalConfig) {
    this.generalConfig = generalConfig;
  }

  public MongoDbShardingConfigBuilder(Injector essentialInjector, Supervisor supervisor) {
    this.generalConfig = new BundleConfigImpl(essentialInjector, supervisor);
  }

  public MongoDbShardingConfigBuilder setTorodBundle(TorodBundle torodBundle) {
    this.torodBundle = torodBundle;
    return this;
  }

  public MongoDbShardingConfigBuilder setUserReplFilter(ReplicationFilters userReplFilter) {
    this.userReplFilter = userReplFilter;
    return this;
  }

  public MongoDbShardingConfigBuilder addShard(ShardConfig config) {
    if (shardConfigs.containsKey(config.getShardId())) {
      throw new IllegalArgumentException("The shard " + config.getShardId() + " has been already"
          + "added");
    }
    shardConfigs.put(config.getShardId(), config);
    return this;
  }

  public MongoDbShardingConfig build() {
    Objects.requireNonNull(torodBundle, "The torod bundle must be not null");
    Objects.requireNonNull(userReplFilter, "The user filter must be not null");
    if (shardConfigs.isEmpty()) {
      throw new IllegalArgumentException("At least one shard is required");
    }

    return new MongoDbShardingConfig(
        torodBundle,
        new ArrayList<>(shardConfigs.values()),
        userReplFilter,
        generalConfig
    );
  }

}
