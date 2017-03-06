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

package com.torodb.mongodb.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.core.bundle.BundleConfigImpl;
import com.torodb.core.logging.DefaultLoggerFactory;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.torod.MemoryTorodBundle;
import com.torodb.torod.TorodBundle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.Optional;


public class MongoDbCoreBundleTest {

  private BundleConfig generalConfig;
  private TorodBundle torodBundle;
  private MongoDbCoreBundle bundle;

  @Before
  public void setUp() {
    Supervisor supervisor = new Supervisor() {
      @Override
      public SupervisorDecision onError(Object supervised, Throwable error) {
        throw new AssertionError("error on " + supervised, error);
      }
    };
    Injector essentialInjector = Guice.createInjector(
        new EssentialModule(
            DefaultLoggerFactory.getInstance(),
            () -> true,
            Clock.systemUTC()
        )
    );

    generalConfig = new BundleConfigImpl(essentialInjector, supervisor);
    torodBundle = new MemoryTorodBundle(generalConfig);

    torodBundle.startAsync();
    torodBundle.awaitRunning();

    MongoDbCoreConfig config = MongoDbCoreConfig.simpleNonServerConfig(
        torodBundle,
        DefaultLoggerFactory.getInstance(),
        Optional.empty(),
        new BundleConfigImpl(essentialInjector, supervisor)
    );

    bundle = new MongoDbCoreBundle(config);
  }

  @After
  public void tearDown() {
    if (torodBundle != null && torodBundle.isRunning()) {
      torodBundle.stopAsync();
    }
  }

  @Test
  public void testStartAndStop() {
    bundle.start()
        .thenCompose((o) -> bundle.stop())
        .join();
    assertThat(bundle.state(), is(Service.State.TERMINATED));
  }
}
