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

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.backend.derby.DerbyDbBackendBundle;
import com.torodb.backend.driver.derby.DerbyDbBackendConfigBuilder;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.mongodb.repl.MongoDbReplConfigBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Clock;

public class StampedeServiceTest {

  private StampedeConfig stampedeConfig;

  @SuppressWarnings("checkstyle:JavadocMethod")
  @Before
  public void setUp() {
    stampedeConfig = new StampedeConfig(
        createEssentialInjector(),
        this::createBackendBundle,
        this::createConfigBuilder);
  }
  
  private Injector createEssentialInjector() {
    return Guice.createInjector(new EssentialModule(() -> true, Clock.systemUTC()));
  }

  private BackendBundle createBackendBundle(BundleConfig bundleConfig) {
    return new DerbyDbBackendBundle(new DerbyDbBackendConfigBuilder(bundleConfig)
        .build()
    );
  }

  private MongoDbReplConfigBuilder createConfigBuilder(BundleConfig bundleConfig) {
    return new MongoDbReplConfigBuilder(bundleConfig)
        .setMongoClientConfiguration(
            MongoClientConfiguration.unsecure(
                HostAndPort.fromParts("localhost", 27017)
            )
        );
  }

  @Test
  public void testCreateStampedeService() {
    Service stampedeService = new StampedeService(stampedeConfig);
    assert !stampedeService.isRunning();
  }

  @Test
  @Ignore
  public void testCreateStampedeService_run() {
    Service stampedeService = new StampedeService(stampedeConfig);
    stampedeService.startAsync();
    stampedeService.awaitRunning();

    stampedeService.stopAsync();
    stampedeService.awaitTerminated();
  }
}
