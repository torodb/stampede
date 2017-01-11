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

package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.base.Preconditions;
import com.torodb.core.modules.BundleConfig;
import com.torodb.mongodb.core.MongoDbCoreBundle;

public class MongoDbReplConfigBuilder {

  private MongoDbCoreBundle coreBundle;
  private MongoClientConfiguration mongoClientConfiguration;
  private ReplicationFilters replicationFilters;
  private String replSetName;
  private ConsistencyHandler consistencyHandler;
  private final BundleConfig generalConfig;

  public MongoDbReplConfigBuilder(BundleConfig generalConfig) {
    this.generalConfig = generalConfig;
  }

  public MongoDbReplConfigBuilder setCoreBundle(MongoDbCoreBundle coreBundle) {
    this.coreBundle = coreBundle;
    return this;
  }

  public MongoDbReplConfigBuilder setMongoClientConfiguration(
      MongoClientConfiguration mongoClientConfiguration) {
    this.mongoClientConfiguration = mongoClientConfiguration;
    return this;
  }

  public MongoDbReplConfigBuilder setReplicationFilters(ReplicationFilters replicationFilters) {
    this.replicationFilters = replicationFilters;
    return this;
  }

  public MongoDbReplConfigBuilder setReplSetName(String replSetName) {
    this.replSetName = replSetName;
    return this;
  }

  public MongoDbReplConfigBuilder setConsistencyHandler(ConsistencyHandler consistencyHandler) {
    this.consistencyHandler = consistencyHandler;
    return this;
  }

  public MongoDbReplConfig build() {
    Preconditions.checkNotNull(coreBundle);
    Preconditions.checkNotNull(mongoClientConfiguration);
    Preconditions.checkNotNull(replicationFilters);
    Preconditions.checkNotNull(replSetName);
    Preconditions.checkNotNull(consistencyHandler);
    Preconditions.checkNotNull(generalConfig);
    return new MongoDbReplConfig(coreBundle, mongoClientConfiguration, replicationFilters,
        replSetName, consistencyHandler, generalConfig);
  }

}
