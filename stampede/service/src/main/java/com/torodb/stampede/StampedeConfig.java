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

package com.torodb.stampede;

import com.google.inject.Injector;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.engine.mongodb.sharding.MongoDbShardingConfig;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.filters.ReplicationFilters;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

public class StampedeConfig {

  private final Injector essentialInjector;
  private final Function<BundleConfig, BackendBundle> backendBundleGenerator;
  private final ReplicationFilters userReplFilters;
  private final List<ShardConfigBuilder> shardConfigBuilders;

  public StampedeConfig(Injector essentialInjector,
      Function<BundleConfig, BackendBundle> backendBundleGenerator,
      ReplicationFilters userReplFilters, List<ShardConfigBuilder> shardConfigBuilders) {
    this.essentialInjector = essentialInjector;
    this.backendBundleGenerator = backendBundleGenerator;
    this.userReplFilters = userReplFilters;
    this.shardConfigBuilders = shardConfigBuilders;
  }

  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  public ThreadFactory getThreadFactory() {
    return getEssentialInjector().getInstance(ThreadFactory.class);
  }

  /**
   * Returns a function used to create {@link BackendBundle backend bundles} given a generic
   * bundle configuration.
   *
   * <p>This is an abstraction that disjoins specific backend configuration (usually specified on
   * the main module by reading a config file) and the {@link StampedeService} that uses the
   * bundle.
   */
  public Function<BundleConfig, BackendBundle> getBackendBundleGenerator() {
    return backendBundleGenerator;
  }

  public ReplicationFilters getUserReplicationFilters() {
    return userReplFilters;
  }

  public List<ShardConfigBuilder> getShardConfigBuilders() {
    return Collections.unmodifiableList(shardConfigBuilders);
  }

  public static interface ShardConfigBuilder {
    String getShardId();
    
    MongoDbShardingConfig.ShardConfig createConfig(ConsistencyHandler consistencyHandler);
  }
}
