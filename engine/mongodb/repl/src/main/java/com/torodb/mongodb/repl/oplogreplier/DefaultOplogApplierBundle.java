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
import com.google.inject.Injector;
import com.torodb.core.modules.AbstractBundle;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;

import java.util.Collection;

public class DefaultOplogApplierBundle extends AbstractBundle<DefaultOplogApplierBundleExtInt> {

  private final DefaultOplogApplierBundleConfig config;
  private final AnalyzedOplogBatchExecutor aoe;
  private final OplogApplier oplogApplier;

  public DefaultOplogApplierBundle(DefaultOplogApplierBundleConfig config) {
    super(config);
    this.config = config;
    Injector injector = config.getEssentialInjector().createChildInjector(
        new DefaultOplogApplierGuiceModule(config)
    );
    oplogApplier = injector.getInstance(OplogApplier.class);
    aoe = injector.getInstance(AnalyzedOplogBatchExecutor.class);
  }

  @Override
  protected void postDependenciesStartUp() throws Exception {
    aoe.startAsync();
    aoe.awaitRunning();
  }

  @Override
  protected void preDependenciesShutDown() throws Exception {
    aoe.stopAsync();
    aoe.awaitTerminated();
  }

  @Override
  public Collection<Service> getDependencies() {
    return Lists.newArrayList(
        config.getMongoDbCoreBundle(),
        config.getReplCoreBundle()
    );
  }

  @Override
  public DefaultOplogApplierBundleExtInt getExternalInterface() {
    return new DefaultOplogApplierBundleExtInt(oplogApplier);
  }

}
