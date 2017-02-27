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
import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;

public class ReplCoreConfig implements BundleConfig {

  private final MongoClientConfiguration mongoClientConfig;
  private final ToroDbReplicationFilters replicationFilters;
  private final MongoDbCoreBundle mongoDbCoreBundle;
  private final Injector essentialInjector;
  private final Supervisor replSupervisor;

  public ReplCoreConfig(MongoClientConfiguration mongoClientConfig,
      ToroDbReplicationFilters replicationFilters, MongoDbCoreBundle mongoDbCoreBundle,
      Injector essentialInjector, Supervisor replSupervisor) {
    this.mongoClientConfig = mongoClientConfig;
    this.replicationFilters = replicationFilters;
    this.mongoDbCoreBundle = mongoDbCoreBundle;
    this.essentialInjector = essentialInjector;
    this.replSupervisor = replSupervisor;
  }

  public MongoClientConfiguration getMongoClientConfig() {
    return mongoClientConfig;
  }

  public ToroDbReplicationFilters getReplicationFilters() {
    return replicationFilters;
  }

  public MongoDbCoreBundle getMongoDbCoreBundle() {
    return mongoDbCoreBundle;
  }

  @Override
  public Injector getEssentialInjector() {
    return essentialInjector;
  }

  @Override
  public Supervisor getSupervisor() {
    return replSupervisor;
  }

}
