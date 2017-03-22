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
import com.torodb.core.bundle.BundleConfigImpl;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.engine.mongodb.sharding.MongoDbShardingConfig.ShardConfig;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;

import java.util.Objects;

public abstract class MongoDbShardingConfigBuilder {

  private TorodBundle torodBundle;
  private ReplicationFilters userReplFilter;
  private LoggerFactory lifecycleLoggerFactory;
  private final BundleConfig generalConfig;

  protected MongoDbShardingConfigBuilder(BundleConfig generalConfig) {
    this.generalConfig = generalConfig;
  }

  protected MongoDbShardingConfigBuilder(Injector essentialInjector, Supervisor supervisor) {
    this.generalConfig = new BundleConfigImpl(essentialInjector, supervisor);
  }

  public static MongoDbShardingConfigBuilder createShardedBuilder(BundleConfig generalConfig) {
    return new MultipleShardConfigBuilder(generalConfig);
  }

  public static MongoDbShardingConfigBuilder createUnshardedBuilder(BundleConfig generalConfig) {
    return new UnshardedConfigBuilder(generalConfig);
  }
  
  public MongoDbShardingConfigBuilder setTorodBundle(TorodBundle torodBundle) {
    this.torodBundle = torodBundle;
    return this;
  }

  public MongoDbShardingConfigBuilder setUserReplFilter(ReplicationFilters userReplFilter) {
    this.userReplFilter = userReplFilter;
    return this;
  }

  public MongoDbShardingConfigBuilder setLifecycleLoggerFactory(
      LoggerFactory lifecycleLoggerFactory) {
    this.lifecycleLoggerFactory = lifecycleLoggerFactory;
    return this;
  }

  public abstract MongoDbShardingConfigBuilder addShard(ShardConfig config);

  protected abstract MongoDbShardingConfig build(TorodBundle torodBundle,
      ReplicationFilters userReplFilter,
      LoggerFactory lifecycleLoggerFactory,
      BundleConfig generalConfig);

  public MongoDbShardingConfig build() {
    Objects.requireNonNull(torodBundle, "The torod bundle must be not null");
    Objects.requireNonNull(userReplFilter, "The user filter must be not null");
    Objects.requireNonNull(lifecycleLoggerFactory, "The lifecycle logger factory must be not null");

    return build(torodBundle, userReplFilter, lifecycleLoggerFactory, generalConfig);
  }

}
