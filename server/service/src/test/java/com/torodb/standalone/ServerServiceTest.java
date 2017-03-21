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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.backend.derby.DerbyDbBackendBundle;
import com.torodb.backend.derby.driver.DerbyDbBackendConfigBuilder;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.logging.ComponentLoggerFactory;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.mongodb.wp.MongoDbWpBundle;
import com.torodb.mongodb.wp.MongoDbWpConfig;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

public class ServerServiceTest {

  private ServerConfig config;
  private final HostAndPort selfHostAndPort = HostAndPort.fromParts("localhost", 27020);

  @SuppressWarnings("checkstyle:JavadocMethod")
  @Before
  public void setUp() {
    config = new ServerConfig(
        createEssentialInjector(),
        this::createBackendBundle,
        selfHostAndPort,
        this::createConfigBuilder,
        new ComponentLoggerFactory("LIFECYCLE")
    );
  }

  private Injector createEssentialInjector() {
    return Guice.createInjector(new EssentialModule(
        DefaultLoggerFactory.getInstance(),
        () -> true,
        Clock.systemUTC())
    );
  }

  private BackendBundle createBackendBundle(BundleConfig bundleConfig) {
    return new DerbyDbBackendBundle(new DerbyDbBackendConfigBuilder(bundleConfig)
        .build()
    );
  }

  private MongoDbWpBundle createConfigBuilder(BundleConfig bundleConfig,
      MongoDbCoreBundle mongoDbCoreBundle) {
    return new MongoDbWpBundle(
        new MongoDbWpConfig(mongoDbCoreBundle, selfHostAndPort.getPort(), bundleConfig)
    );
  }
  
  @Test(timeout = 60_000)
  public void testCreateStampedeService() {
    new ServerService(config);
  }

  @Test(timeout = 60_000)
  public void testCreateStampedeService_run() {
    Service stampedeService = new ServerService(config);
    stampedeService.startAsync();
    stampedeService.awaitRunning();

    stampedeService.stopAsync();
    stampedeService.awaitTerminated();
  }

}
