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

package com.torodb.standalone;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.bundle.BundleConfigImpl;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.core.MongoDbCoreConfig;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.mongodb.wp.MongoDbWpBundle;
import com.torodb.torod.SqlTorodBundle;
import com.torodb.torod.SqlTorodConfig;
import com.torodb.torod.TorodBundle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * This service is used to start and stop ToroDB Server.
 *
 * <p>It takes a {@link ServerConfig} and uses it to create and start the required
 * {@link Bundle bundles}.
 */
public class ServerService extends AbstractIdleService implements Supervisor {

  private static final Logger LOGGER = LogManager.getLogger(ServerService.class);
  private final ThreadFactory threadFactory;
  private final Injector essentialInjector;
  private final BundleConfig generalBundleConfig;  
  private final ServerConfig config;
  private final Shutdowner shutdowner;

  public ServerService(ServerConfig config) {
    this.config = config;
    this.essentialInjector = config.getEssentialInjector();
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
    this.stopAsync();
    return SupervisorDecision.STOP;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting up ToroDB Server");

    shutdowner.startAsync();
    shutdowner.awaitRunning();

    BackendBundle backendBundle = config.getBackendBundleGenerator().apply(generalBundleConfig);
    startBundle(backendBundle);

    TorodBundle torodBundle = createTorodBundle(backendBundle);
    startBundle(torodBundle);

    MongoDbCoreBundle mongoDbCoreBundle = createMongoDbCoreBundle(torodBundle);
    startBundle(mongoDbCoreBundle);

    MongoDbWpBundle mongodbWpBundle = config.getMongoDbWpBundleGenerator()
        .apply(generalBundleConfig, mongoDbCoreBundle);
    startBundle(mongodbWpBundle);

    LOGGER.info("ToroDB Server is now running");
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Shutting down ToroDB Standalone");
    if (shutdowner != null) {
      shutdowner.stopAsync();
      shutdowner.awaitTerminated();
    }
    LOGGER.info("ToroDB Stampede has been shutted down");
  }

  private MongoDbCoreBundle createMongoDbCoreBundle(TorodBundle torodBundle) {
    MongodServerConfig mongodServerConfig = new MongodServerConfig(config.getSelfHostAndPort());
    return new MongoDbCoreBundle(
        MongoDbCoreConfig.simpleConfig(torodBundle, mongodServerConfig, generalBundleConfig)
    );
  }

  private TorodBundle createTorodBundle(BackendBundle backendBundle) {
    return new SqlTorodBundle(new SqlTorodConfig(
        backendBundle,
        essentialInjector,
        this
    ));
  }

  private void startBundle(Service service) {
    service.startAsync();
    service.awaitRunning();

    shutdowner.addStopShutdownListener(service);
  }

}
