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

import com.google.common.util.concurrent.Service;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.model.protocol.mongo.Role;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.packaging.guice.BackendDerbyImplementationModule;
import com.torodb.packaging.guice.BackendImplementationModule;
import com.torodb.stampede.config.model.StampedeConfig;
import com.torodb.stampede.config.model.mongo.replication.Replication;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Clock;
import java.util.Collections;

public class StampedeBootstrapTest {

  private StampedeConfig stampedeConfig;

  @SuppressWarnings("checkstyle:JavadocMethod")
  @Before
  public void setUp() {
    stampedeConfig = new StampedeConfig();

    Replication replication = new Replication();
    replication.setRole(Role.HIDDEN_SLAVE);
    replication.setReplSetName("replSetName");
    replication.setSyncSource("localhost:27020");

    stampedeConfig.setReplication(Collections.singletonList(replication));
    stampedeConfig.getBackend().setBackendImplementation(new Derby());
    stampedeConfig.getBackend().as(Derby.class).setPassword("torodb");
    stampedeConfig.getLogging().setLevel(LogLevel.TRACE);

    ConfigUtils.validateBean(stampedeConfig);
  }

  @Test
  public void testCreateStampedeService() {
    Service stampedeService = new StampedeService(stampedeConfig, Clock.systemUTC());
  }

  @Test
  @Ignore
  public void testCreateStampedeService_run() {
    Service stampedeService = new StampedeService(stampedeConfig, Clock.systemUTC());
    stampedeService.startAsync();
    stampedeService.awaitRunning();

    stampedeService.stopAsync();
    stampedeService.awaitTerminated();
  }

  private static class TestBootstrapModule extends BootstrapModule {

    public TestBootstrapModule(StampedeConfig config, Clock clock) {
      super(config, clock);
    }

    @Override
    protected BackendImplementationModule getBackendImplementationModule() {
      return new BackendDerbyImplementationModule();
    }
  }

  private class Derby extends com.torodb.packaging.config.model.backend.derby.AbstractDerby {

    public Derby() {
      super(
          "localhost",
          1527,
          "torod",
          "torodb",
          null,
          System.getProperty("user.home", "/") + "/.toropass",
          "toro",
          false,
          true,
          true);
    }
  }

}
