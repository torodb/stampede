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

import com.google.inject.*;
import com.torodb.core.modules.Bundle;
import com.torodb.core.modules.BundleConfig;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.engine.essential.EssentialModule;
import com.torodb.mongodb.core.MongoDbCoreBundle;
import com.torodb.mongodb.repl.MongoDbCoreBundleServiceBundle;
import com.torodb.mongodb.repl.ReplCoreBundle;
import com.torodb.mongodb.repl.ReplCoreBundleTest;
import com.torodb.mongodb.repl.TestBundleConfig;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import java.time.Clock;

/**
 * A test rule that initializes the {@link OplogTestContext}.
 */
public class OplogTestContextResourceRule extends ExternalResource {

  private final OplogApplierBundleFactory bundleFactory;

  private final Supervisor supervisor = new Supervisor() {
    @Override
    public SupervisorDecision onError(Object supervised, Throwable error) {
      Assert.fail(error.getLocalizedMessage());
      return SupervisorDecision.STOP;
    }
  };
  private final Injector essentialInjector = Guice.createInjector(new EssentialModule(
      () -> false,
      Clock.systemUTC())
  );

  private MongoDbCoreBundleServiceBundle mongoDbCoreBundleServiceBundle;
  private ReplCoreBundle replCoreBundle;
  private Bundle<OplogApplier> applierBundle;
  private OplogTestContext testContext;

  public OplogTestContextResourceRule(OplogApplierBundleFactory bundleFactory) {
    this.bundleFactory = bundleFactory;
  }

  public OplogTestContext getTestContext() {
    return testContext;
  }

  @Override
  protected void before() throws Throwable {
    mongoDbCoreBundleServiceBundle = MongoDbCoreBundleServiceBundle.createBundle();
    mongoDbCoreBundleServiceBundle.start().join();
    
    MongoDbCoreBundle coreBundle = mongoDbCoreBundleServiceBundle.getExternalInterface();
    assert coreBundle.isRunning();

    BundleConfig generalConfig = new TestBundleConfig();

    replCoreBundle = ReplCoreBundleTest.createBundle(generalConfig, coreBundle);
    replCoreBundle.startAsync();

    applierBundle = bundleFactory.createOplogApplierBundle(
        generalConfig,
        replCoreBundle,
        coreBundle
    );
    applierBundle.startAsync();
    applierBundle.awaitRunning();

    testContext = new DefaultOplogTestContext(
        coreBundle.getExternalInterface().getMongodServer(),
        applierBundle.getExternalInterface()
    );
  }

  @Override
  protected void after() {
    if (applierBundle != null && applierBundle.isRunning()) {
      applierBundle.stop().join();
      applierBundle = null;
    }
    if (replCoreBundle != null && replCoreBundle.isRunning()) {
      replCoreBundle.stop().join();
      replCoreBundle = null;
    }
    if (mongoDbCoreBundleServiceBundle != null && mongoDbCoreBundleServiceBundle.isRunning()) {
      mongoDbCoreBundleServiceBundle.stop().join();
      mongoDbCoreBundleServiceBundle = null;
    }
  }

  /**
   * A functional interface used to create the {@link OplogApplierBundle} that will be used to
   * execute the test.
   */
  @FunctionalInterface
  public static interface OplogApplierBundleFactory {
    public Bundle<OplogApplier> createOplogApplierBundle(BundleConfig generalConfig,
      ReplCoreBundle replCoreBundle, MongoDbCoreBundle mongoDbCoreBundle);
  }
}
