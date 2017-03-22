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

import com.google.common.util.concurrent.Service;
import com.torodb.core.logging.ComponentLoggerFactory;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.repl.MongoDbReplBundle;
import com.torodb.mongodb.repl.MongoDbReplConfig;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;
import com.torodb.torod.TorodBundle;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class UnshardedShardBundle extends ShardBundle {

  private final TorodBundle actualTorodBundle;
  private final MongoDbCoreBundle coreBundle;
  private final MongoDbReplBundle replBundle;

  public UnshardedShardBundle(ShardBundleConfig config) {
    super(config);

    actualTorodBundle = config.getTorodBundle();

    coreBundle = new MongoDbCoreBundle(
        MongoDbCoreConfig.simpleNonServerConfig(
            actualTorodBundle,
            new ComponentLoggerFactory("MONGOD"),
            Optional.empty(),
            config
        )
    );
    replBundle = new MongoDbReplBundle(createReplConfig(config, coreBundle));
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    coreBundle.startAsync();
    coreBundle.awaitRunning();

    replBundle.startAsync();
    replBundle.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    replBundle.stopAsync();
    replBundle.awaitTerminated();

    coreBundle.stopAsync();
    coreBundle.awaitTerminated();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Collections.singleton(actualTorodBundle);
  }

  private static MongoDbReplConfig createReplConfig(
      ShardBundleConfig config,
      MongoDbCoreBundle coreBundle) {
    return new MongoDbReplConfigBuilder(config)
        .setConsistencyHandler(config.getConsistencyHandler())
        .setCoreBundle(coreBundle)
        .setMongoClientConfiguration(config.getClientConfig())
        .setReplSetName(config.getReplSetName())
        .setReplicationFilters(config.getUserReplFilter())
        .setMetricRegistry(Optional.empty())
        .setLoggerFactory(new ComponentLoggerFactory("REPL"))
        .build();
  }

}
