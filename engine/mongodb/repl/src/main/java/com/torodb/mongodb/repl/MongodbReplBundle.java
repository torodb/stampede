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
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.core.modules.Bundle;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.repl.guice.MongoDbReplModule;
import com.torodb.mongodb.repl.guice.MongodbReplConfig;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.topology.TopologyService;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.torod.TorodBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class MongodbReplBundle extends AbstractBundle {

  private static final Logger LOGGER =
      LogManager.getLogger(MongodbReplBundle.class);
  private final TorodBundle torodBundle;
  private final TopologyService topologyService;
  private final ReplCoordinator replCoordinator;
  private final OplogManager oplogManager;
  private final MongodServer mongodServer;
  private final CachedMongoClientFactory cachedMongoClientFactory;
  private final DbCloner dbCloner;
  private final AnalyzedOplogBatchExecutor aobe;

  public MongodbReplBundle(TorodBundle torodBundle, Supervisor supervisor,
      MongodbReplConfig config, Injector injector) {
    super(
        injector.getInstance(
            Key.get(ThreadFactory.class, TorodbIdleService.class)),
        supervisor);

    Supervisor replSupervisor = new ReplSupervisor(supervisor);

    Injector replInjector = injector.createChildInjector(
        new MongoDbReplModule(config, replSupervisor)
    );

    this.torodBundle = torodBundle;
    this.topologyService = replInjector.getInstance(TopologyService.class);
    this.replCoordinator = replInjector.getInstance(ReplCoordinator.class);
    this.oplogManager = replInjector.getInstance(OplogManager.class);
    this.mongodServer = replInjector.getInstance(MongodServer.class);
    this.cachedMongoClientFactory = replInjector.getInstance(CachedMongoClientFactory.class);
    this.dbCloner = replInjector.getInstance(Key.get(DbCloner.class, MongoDbRepl.class));
    this.aobe = replInjector.getInstance(AnalyzedOplogBatchExecutor.class);
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    LOGGER.info("Starting replication service");

    mongodServer.startAsync();
    mongodServer.awaitRunning();

    topologyService.startAsync();
    oplogManager.startAsync();

    topologyService.awaitRunning();
    oplogManager.awaitRunning();

    dbCloner.startAsync();
    dbCloner.awaitRunning();

    aobe.startAsync();
    aobe.awaitRunning();

    replCoordinator.startAsync();
    replCoordinator.awaitRunning();

    LOGGER.info("Replication service started");
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    LOGGER.info("Shutting down replication service");

    LOGGER.debug("Shutting down replication layer");
    try {
      replCoordinator.stopAsync();
      replCoordinator.awaitTerminated();
    } catch (IllegalStateException ex) {
      Preconditions.checkState(!replCoordinator.isRunning(),
          "It was expected that {} was not running", replCoordinator);
    }

    aobe.stopAsync();
    aobe.awaitTerminated();

    dbCloner.stopAsync();
    dbCloner.awaitTerminated();

    oplogManager.stopAsync();
    topologyService.stopAsync();

    try {
      oplogManager.awaitTerminated();
    } catch (IllegalStateException ex) {
      Preconditions.checkState(!oplogManager.isRunning(),
          "It was expected that {} was not running", replCoordinator);
    }
    try {
      topologyService.awaitTerminated();
    } catch (IllegalStateException ex) {
      Preconditions.checkState(!topologyService.isRunning(),
          "It was expected that {} was not running", replCoordinator);
    }

    LOGGER.debug("Replication layer has been shutted down");

    mongodServer.stopAsync();

    LOGGER.debug("Closing remote connections");
    cachedMongoClientFactory.invalidateAll();
    LOGGER.debug("Remote connections have been closed");

    try {
      mongodServer.awaitTerminated();
    } catch (IllegalStateException ex) {
      Preconditions.checkState(!mongodServer.isRunning(), "It was expected that {} was not running",
          replCoordinator);
    }

    LOGGER.info("Replication service shutted down");
  }

  @Override
  public Collection<Bundle> getDependencies() {
    return Collections.singleton(torodBundle);
  }

  public MongodServer getMongodServer() {
    return mongodServer;
  }

  private class ReplSupervisor implements Supervisor {

    private final Supervisor supervisor;

    public ReplSupervisor(Supervisor supervisor) {
      this.supervisor = supervisor;
    }

    @Override
    public SupervisorDecision onError(Object supervised, Throwable error) {
      LOGGER.error("Catched an error on the replication layer. Escalating it");
      SupervisorDecision decision = supervisor.onError(this, error);
      if (decision == SupervisorDecision.STOP) {
        MongodbReplBundle.this.stopAsync();
      }
      return decision;
    }

    @Override
    public String toString() {
      return "replication supervisor";
    }
  }
}
