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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.modules.Bundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.modules.BundleConfigImpl;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.MongoDbReplBundle;
import com.torodb.torod.SqlTorodBundle;
import com.torodb.torod.SqlTorodConfig;
import com.torodb.torod.TorodBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * This service is used to start and stop ToroDB Stampede.
 *
 * <p>It takes a {@link StampedeConfig} and uses it to create and start the required
 * {@link Bundle bundles}.
 */
public class StampedeService extends AbstractIdleService implements Supervisor {

  private static final Logger LOGGER = LogManager.getLogger(StampedeService.class);
  private final ThreadFactory threadFactory;
  private final StampedeConfig stampedeConfig;
  private final Injector essentialInjector;
  private final BundleConfig generalBundleConfig;
  private final Shutdowner shutdowner;

  public StampedeService(StampedeConfig stampedeConfig) {
    this.stampedeConfig = stampedeConfig;

    this.essentialInjector = stampedeConfig.getEssentialInjector();
    this.threadFactory = essentialInjector.getInstance(ThreadFactory.class);
    this.generalBundleConfig = new BundleConfigImpl(essentialInjector, this);
    this.shutdowner = essentialInjector.getInstance(Shutdowner.class);
  }

  @Override
  protected Executor executor() {
    return (Runnable command) -> {
      Thread thread = threadFactory.newThread(command);
      thread.start();
    };
  }

  @Override
  public SupervisorDecision onError(Object supervised, Throwable error) {
    LOGGER.error("Error reported by " + supervised + ". Stopping ToroDB Stampede", error);
    this.stopAsync();
    return SupervisorDecision.IGNORE;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting up ToroDB Stampede");

    shutdowner.startAsync();
    shutdowner.awaitRunning();
    
    BackendBundle backendBundle = stampedeConfig.getBackendBundleGenerator()
        .apply(generalBundleConfig);
    startBundle(backendBundle);
    
    ConsistencyHandler consistencyHandler = createConsistencyHandler(backendBundle);
    if (!consistencyHandler.isConsistent()) {
      dropDatabase(backendBundle);
    }

    TorodBundle torodBundle = createTorodBundle(backendBundle);
    startBundle(torodBundle);

    MongoDbCoreBundle mongoCoreBundle = createMongoDbCoreBundle(torodBundle);
    startBundle(mongoCoreBundle);

    MongoDbReplBundle replBundle = createMongoDbReplBundle(mongoCoreBundle, consistencyHandler);
    startBundle(replBundle);
    
    LOGGER.info("ToroDB Stampede is now running");
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Shutting down ToroDB Stampede");
    if (shutdowner != null) {
      shutdowner.stopAsync();
      shutdowner.awaitTerminated();
    }
    LOGGER.info("ToroDB Stampede has been shutted down");
  }

  private ConsistencyHandler createConsistencyHandler(BackendBundle backendBundle) {
    Retrier retrier = essentialInjector.getInstance(Retrier.class);
    return new DefaultConsistencyHandler(
        backendBundle.getExternalInterface().getBackendService(),
        retrier
    );
  }

  private TorodBundle createTorodBundle(BackendBundle backendBundle) {
    return new SqlTorodBundle(new SqlTorodConfig(
        backendBundle,
        essentialInjector,
        this
    ));
  }

  private MongoDbCoreBundle createMongoDbCoreBundle(TorodBundle torodBundle) {
    /*
     * The following config file is used by command implementations like isMaster to return
     * information about the server. That has no sense on Stampede and, in fact, that command is
     * never executed. Ideally, implementations like that one should be implemented on the ToroDB
     * Server layer, but right now almost all commands must be implemented on the mongodb core
     * layer, which means we need to provide a value even if it is not used.
     */
    MongodServerConfig mongodServerConfig = new MongodServerConfig(
        HostAndPort.fromParts("localhost", 27017)
    );
    return new MongoDbCoreBundle(
        MongoDbCoreConfig.simpleConfig(torodBundle, mongodServerConfig, generalBundleConfig)
    );
  }

  private MongoDbReplBundle createMongoDbReplBundle(MongoDbCoreBundle coreBundle,
      ConsistencyHandler consistencyHandler) {
    return new MongoDbReplBundle(
        stampedeConfig.getReplBundleConfigBuilderGenerator()
            .apply(generalBundleConfig)
            .setConsistencyHandler(consistencyHandler)
            .setCoreBundle(coreBundle)
            .build()
    );
  }

  private void dropDatabase(BackendBundle backendBundle) throws UserException {
    BackendService backendService = backendBundle.getExternalInterface().getBackendService();
    try (BackendConnection conn = backendService.openConnection();
        ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {
      trans.dropUserData();
      trans.commit();
    }
  }

  private void startBundle(Bundle<?> bundle) {
    bundle.startAsync();
    bundle.awaitRunning();

    shutdowner.addStopShutdownListener(bundle);
  }
}
