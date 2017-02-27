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

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.inject.Injector;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;

import java.util.List;
import java.util.Objects;

public class MongoDbShardingConfig implements BundleConfig  {

  private final TorodBundle torodBundle;
  private final List<ShardConfig> shardConfigs;
  private final ReplicationFilters userReplFilter;
  private final BundleConfig generalConfig;

  MongoDbShardingConfig(TorodBundle torodBundle, List<ShardConfig> shardConfigs,
      ReplicationFilters userReplFilter, BundleConfig generalConfig) {
    this.torodBundle = torodBundle;
    this.shardConfigs = shardConfigs;
    this.userReplFilter = userReplFilter;
    this.generalConfig = generalConfig;
  }

  public TorodBundle getTorodBundle() {
    return torodBundle;
  }

  @DoNotChange
  public List<ShardConfig> getShardConfigs() {
    return shardConfigs;
  }

  public ReplicationFilters getUserReplFilter() {
    return userReplFilter;
  }

  @Override
  public Injector getEssentialInjector() {
    return generalConfig.getEssentialInjector();
  }

  @Override
  public Supervisor getSupervisor() {
    return generalConfig.getSupervisor();
  }

  public static class ShardConfig {
    private final String shardId;
    private final MongoClientConfiguration clientConfig;
    private final String replSetName;
    private final ConsistencyHandler consistencyHandler;

    public ShardConfig(String shardId, MongoClientConfiguration clientConfig, String replSetName,
        ConsistencyHandler consistencyHandler) {
      this.shardId = shardId;
      this.clientConfig = clientConfig;
      this.replSetName = replSetName;
      this.consistencyHandler = consistencyHandler;
    }

    public String getShardId() {
      return shardId;
    }

    public MongoClientConfiguration getClientConfig() {
      return clientConfig;
    }

    public String getReplSetName() {
      return replSetName;
    }

    public ConsistencyHandler getConsistencyHandler() {
      return consistencyHandler;
    }

    @Override
    public int hashCode() {
      int hash = 5;
      hash = 23 * hash + Objects.hashCode(this.shardId);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ShardConfig other = (ShardConfig) obj;
      if (!Objects.equals(this.shardId, other.shardId)) {
        return false;
      }
      return true;
    }
  }
 
}
