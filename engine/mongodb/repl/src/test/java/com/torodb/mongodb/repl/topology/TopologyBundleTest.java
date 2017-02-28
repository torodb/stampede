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

package com.torodb.mongodb.repl.topology;


import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.AbstractReplBundleTest;
import com.torodb.mongodb.repl.MongoDbCoreBundleServiceBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundleTest;
import com.torodb.mongodb.repl.TestBundleConfig;
import com.torodb.mongodb.repl.TestReplEssentialOverrideModule;
import com.torodb.mongodb.repl.guice.ReplEssentialOverrideModule;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TopologyBundleTest extends AbstractReplBundleTest<TopologyBundle> {

  private TopologyBundle bundle;

  @Override
  public TopologyBundle getBundle() {
    assert bundle != null;
    return bundle;
  }

  @Before
  public void setUp() {
    TestBundleConfig generalConfig = new TestBundleConfig();

    MongoDbCoreBundleServiceBundle mongoCoreBundleFactory = MongoDbCoreBundleServiceBundle.createBundle();
    MongoDbCoreBundle mongoCoreBundle = mongoCoreBundleFactory.getExternalInterface();
    ReplCoreBundle replCoreBundle = ReplCoreBundleTest.createBundle(generalConfig, mongoCoreBundle);

    List<Service> dependencies = Lists.newArrayList(
        mongoCoreBundleFactory,
        replCoreBundle
    );

    setDependencies(dependencies);

    bundle = createBundle(
        generalConfig,
        replCoreBundle
    );
  }

  public static TopologyBundle createBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle) {
    return createBundle(generalConfig, replCoreBundle, HostAndPort.fromParts("localhost", 27017));
  }

  public static TopologyBundle createBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle, HostAndPort seed) {

    ReplEssentialOverrideModule essentialOverrideModule = new TestReplEssentialOverrideModule(
        generalConfig.getEssentialInjector()
    );

    return new TopologyBundle(new TopologyBundleConfig(
        replCoreBundle.getExternalInterface().getMongoClientFactory(),
        "replSetName1",
        seed,
        essentialOverrideModule,
        generalConfig)
    );
  }

  @Override
  @Test
  public void testStartAndStop() {
    //This bundle requires remotes nodes, so it cannot start without them
    //As there are no clients, the bundle start should not end. Therefore a timeout is added.
    //If the timeout is reach, then we can assume it worked as expected.

    assert !bundle.isRunning();

    boolean timeout = false;
    bundle.startAsync();
    try {
      bundle.awaitTerminated(1, TimeUnit.SECONDS);
    } catch (TimeoutException ignore) {
      timeout = true;
    }
    if (!timeout) {
      assert bundle.isRunning();
    }
  }
}
