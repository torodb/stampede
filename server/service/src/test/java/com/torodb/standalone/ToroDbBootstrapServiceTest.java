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

import com.google.common.util.concurrent.Service;
import com.torodb.packaging.config.model.generic.LogLevel;
import com.torodb.packaging.config.util.ConfigUtils;
import com.torodb.standalone.config.model.Config;
import com.torodb.standalone.config.model.backend.derby.Derby;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Clock;

/**
 *
 */
public class ToroDbBootstrapServiceTest {

  private Config config;

  @Before
  public void setUp() {
    config = new Config();

    config.getProtocol().getMongo().setReplication(null);
    config.getBackend().setBackendImplementation(new Derby());
    config.getBackend().as(Derby.class).setPassword("torodb");
    config.getGeneric().setLogLevel(LogLevel.TRACE);

    ConfigUtils.validateBean(config);
  }

  @Test
  public void testCreateStampedeService() {
    ToroDbBootstrap.createStandaloneService(config, Clock.systemUTC());
  }

  @Test
  @Ignore(value = "The test is not working properly")
  public void testCreateStampedeService_run() {
    Service stampedeService = ToroDbBootstrap.createStandaloneService(
        config,
        Clock.systemUTC());
    stampedeService.startAsync();
    stampedeService.awaitRunning();

    stampedeService.stopAsync();
    stampedeService.awaitTerminated();
  }

}
