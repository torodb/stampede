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

import com.eightkdata.mongowp.client.core.CachedMongoClientFactory;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.bundle.AbstractBundle;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;
import com.torodb.mongodb.repl.guice.ReplCoreModule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class ReplCoreBundle extends AbstractBundle<ReplCoreExtInt> {

  private static final Logger LOGGER = LogManager.getLogger(ReplCoreBundle.class);
  private final ReplCoreConfig replCoreConfig;
  private final OplogManager oplogManager;
  private final CachedMongoClientFactory mongoClientFactory;
  private final OplogReaderProvider oplogReaderProvider;
  private final ReplMetrics replMetrics;
  private final ToroDbReplicationFilters replicationFilters;

  public ReplCoreBundle(ReplCoreConfig replCoreConfig) {
    super(replCoreConfig);
    this.replCoreConfig = replCoreConfig;

    Injector injector = replCoreConfig.getEssentialInjector()
        .createChildInjector(new ReplCoreModule(replCoreConfig));
    oplogManager = injector.getInstance(OplogManager.class);
    mongoClientFactory = injector.getInstance(CachedMongoClientFactory.class);
    oplogReaderProvider = injector.getInstance(OplogReaderProvider.class);
    replMetrics = injector.getInstance(ReplMetrics.class);
    replicationFilters = replCoreConfig.getReplicationFilters();
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.debug("Starting oplog manager");
    oplogManager.startAsync();
    oplogManager.awaitRunning();
    LOGGER.debug("Oplog manager started");
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    LOGGER.debug("Shutting down oplog manager");
    oplogManager.stopAsync();
    oplogManager.awaitTerminated();
    LOGGER.debug("Oplog manager Shutted down");

    LOGGER.debug("Closing remote connections");
    mongoClientFactory.invalidateAll();
    LOGGER.debug("Remote connections have been closed");
  }

  @Override
  public Collection<Service> getDependencies() {
    return Lists.newArrayList(replCoreConfig.getMongoDbCoreBundle());
  }

  @Override
  public ReplCoreExtInt getExternalInterface() {
    return new ReplCoreExtInt(oplogManager, mongoClientFactory, oplogReaderProvider, replMetrics,
      replicationFilters
    );
  }

}
