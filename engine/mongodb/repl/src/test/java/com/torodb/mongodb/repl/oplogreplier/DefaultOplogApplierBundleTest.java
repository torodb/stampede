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

package com.torodb.mongodb.repl.oplogreplier;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.torodb.core.bundle.BundleConfig;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.AbstractReplBundleTest;
import com.torodb.mongodb.repl.MongoDbCoreBundleServiceBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundleTest;
import com.torodb.mongodb.repl.TestBundleConfig;
import com.torodb.mongodb.repl.TestReplEssentialOverrideModule;
import com.torodb.mongodb.repl.commands.ReplCommandsBuilder;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;
import com.torodb.mongodb.repl.guice.ReplEssentialOverrideModule;
import org.junit.Before;

import java.util.List;


public class DefaultOplogApplierBundleTest extends AbstractReplBundleTest<DefaultOplogApplierBundle> {

  private DefaultOplogApplierBundle bundle;

  @Override
  public DefaultOplogApplierBundle getBundle() {
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
        replCoreBundle,
        mongoCoreBundle,
        new ToroDbReplicationFilters()
    );
  }

  public static DefaultOplogApplierBundle createBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle, MongoDbCoreBundle mongoDbCoreBundle,
      ToroDbReplicationFilters replFilters) {

    ReplEssentialOverrideModule essentialOverrideModule = new TestReplEssentialOverrideModule(
        generalConfig.getEssentialInjector()
    );

    ReplCommandsBuilder testReplCommandsUtil = new ReplCommandsBuilder(
        generalConfig,
        replFilters,
        essentialOverrideModule
    );
    
    return new DefaultOplogApplierBundle(new DefaultOplogApplierBundleConfig(
        replCoreBundle,
        mongoDbCoreBundle,
        testReplCommandsUtil.getReplCommandsLibrary(),
        testReplCommandsUtil.getReplCommandsExecutor(),
        essentialOverrideModule,
        generalConfig)
    );
  }

}
