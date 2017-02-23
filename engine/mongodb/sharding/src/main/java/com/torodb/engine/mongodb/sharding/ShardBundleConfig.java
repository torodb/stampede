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
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.filters.ReplicationFilters;
import com.torodb.torod.TorodBundle;

public class ShardBundleConfig extends BundleConfigImpl {
  private final String shardId;
  private final TorodBundle torodBundle;
  private final MongoClientConfiguration clientConfig;
  private final String replSetName;
  private final ReplicationFilters userReplFilter;
  private final ConsistencyHandler consistencyHandler;

  public ShardBundleConfig(String shardId, TorodBundle torodBundle, 
      MongoClientConfiguration clientConfig, String replSetName, ReplicationFilters userReplFilter,
      ConsistencyHandler consistencyHandler, Injector essentialInjector, Supervisor supervisor) {
    super(essentialInjector, supervisor);
    this.shardId = shardId;
    this.torodBundle = torodBundle;
    this.clientConfig = clientConfig;
    this.replSetName = replSetName;
    this.userReplFilter = userReplFilter;
    this.consistencyHandler = consistencyHandler;
  }

  public String getShardId() {
    return shardId;
  }

  public TorodBundle getTorodBundle() {
    return torodBundle;
  }

  public MongoClientConfiguration getClientConfig() {
    return clientConfig;
  }

  public ConsistencyHandler getConsistencyHandler() {
    return consistencyHandler;
  }

  public String getReplSetName() {
    return replSetName;
  }

  public ReplicationFilters getUserReplFilter() {
    return userReplFilter;
  }


}
