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


import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;
import com.torodb.mongodb.repl.guice.ReplEssentialOverrideModule;
import org.junit.Before;

import java.util.List;


public class ReplCoreBundleTest extends AbstractReplBundleTest<ReplCoreBundle>{

  private ReplCoreBundle bundle;

  @Override
  public ReplCoreBundle getBundle() {
    assert bundle != null;
    return bundle;
  }

  @Before
  public void setUp() {
    TestBundleConfig generalConfig = new TestBundleConfig();

    MongoDbCoreBundleServiceBundle mongoCoreBundleFactory = MongoDbCoreBundleServiceBundle.createBundle();
    MongoDbCoreBundle mongoCoreBundle = mongoCoreBundleFactory.getExternalInterface();
    
    List<Service> dependencies = Lists.newArrayList(
        mongoCoreBundleFactory
    );

    setDependencies(dependencies);

    bundle = createBundle(generalConfig, mongoCoreBundle);
  }

  public static ReplCoreBundle createBundle(BundleConfig generalConfig,
      MongoDbCoreBundle mongoDbCoreBundle) {
    return createBundle(
        generalConfig,
        mongoDbCoreBundle,
        HostAndPort.fromParts("localhost", 27017)
    );
  }

  public static ReplCoreBundle createBundle(BundleConfig generalConfig,
      MongoDbCoreBundle mongoDbCoreBundle, HostAndPort seed) {

    ReplEssentialOverrideModule essentialOverrideModule = new TestReplEssentialOverrideModule(
        generalConfig.getEssentialInjector()
    );

    return new ReplCoreBundle(new ReplCoreConfig(
        MongoClientConfiguration.unsecure(seed),
        new ToroDbReplicationFilters(),
        mongoDbCoreBundle,
        essentialOverrideModule,
        generalConfig.getEssentialInjector(),
        generalConfig.getSupervisor())
    );
  }
}
