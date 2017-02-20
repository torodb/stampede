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
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.filters.ReplicationFilters;

import java.util.concurrent.ThreadFactory;

/**
 * The configuration used by {@link MongoDbReplBundle}.
 */
public class MongoDbReplConfig implements BundleConfig {
  private final MongoDbCoreBundle coreBundle;
  private final MongoClientConfiguration mongoClientConfiguration;
  private final ReplicationFilters userReplFilter;
  private final String replSetName;
  private final ConsistencyHandler consistencyHandler;
  private final BundleConfig generalConfig;

  protected MongoDbReplConfig(MongoDbCoreBundle coreBundle,
      MongoClientConfiguration mongoClientConfiguration, ReplicationFilters replicationFilters,
      String replSetName, ConsistencyHandler consistencyHandler, BundleConfig generalConfig) {
    this.coreBundle = coreBundle;
    this.mongoClientConfiguration = mongoClientConfiguration;
    this.userReplFilter = replicationFilters;
    this.replSetName = replSetName;
    this.consistencyHandler = consistencyHandler;
    this.generalConfig = generalConfig;
  }

  public MongoDbCoreBundle getMongoDbCoreBundle() {
    return coreBundle;
  }

  public MongoClientConfiguration getMongoClientConfiguration() {
    return mongoClientConfiguration;
  }

  public ReplicationFilters getUserReplicationFilter() {
    return userReplFilter;
  }

  public String getReplSetName() {
    return replSetName;
  }

  public ConsistencyHandler getConsistencyHandler() {
    return consistencyHandler;
  }

  @Override
  public Injector getEssentialInjector() {
    return generalConfig.getEssentialInjector();
  }

  @Override
  public ThreadFactory getThreadFactory() {
    return generalConfig.getThreadFactory();
  }

  @Override
  public Supervisor getSupervisor() {
    return generalConfig.getSupervisor();
  }

  public HostAndPort getSyncSourceSeed() {
    return getMongoClientConfiguration().getHostAndPort();
  }
}
