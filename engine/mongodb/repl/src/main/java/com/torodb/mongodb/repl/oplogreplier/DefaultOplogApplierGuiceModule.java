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

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.filters.ByNamespaceOplogOperationFilter;
import com.torodb.mongodb.filters.DatabaseFilter;
import com.torodb.mongodb.filters.NamespaceFilter;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.commands.ReplCommandExecutor;
import com.torodb.mongodb.repl.commands.ReplCommandLibrary;
import com.torodb.mongodb.repl.filters.ToroDbReplicationFilters;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.NamespaceJobExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.OplogBatchChecker;
import com.torodb.mongodb.repl.oplogreplier.batch.OplogBatchFilter;

import java.time.Duration;

public class DefaultOplogApplierGuiceModule extends PrivateModule {

  private final DefaultOplogApplierBundleConfig config;

  public DefaultOplogApplierGuiceModule(DefaultOplogApplierBundleConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    expose(OplogApplier.class);
    expose(AnalyzedOplogBatchExecutor.class);

    bindConfig();

    bind(OplogApplier.class)
        .to(DefaultOplogApplier.class)
        .in(Singleton.class);
    bind(DefaultOplogApplier.BatchLimits.class)
        .toInstance(new DefaultOplogApplier.BatchLimits(1000, Duration.ofSeconds(2)));
    bind(OplogApplierMetrics.class)
        .in(Singleton.class);

    bind(ConcurrentOplogBatchExecutor.class)
        .in(javax.inject.Singleton.class);
    bind(AnalyzedOplogBatchExecutor.class)
        .to(ConcurrentOplogBatchExecutor.class);
    bind(ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics.class)
        .in(javax.inject.Singleton.class);
    bind(AnalyzedOplogBatchExecutor.AnalyzedOplogBatchExecutorMetrics.class)
        .to(ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics.class);

    bind(ConcurrentOplogBatchExecutor.SubBatchHeuristic.class)
        .toInstance((metrics) -> 100);

    bind(AnalyzedOpReducer.class)
        .toInstance(new AnalyzedOpReducer(false));
    bind(NamespaceJobExecutor.class)
        .in(Singleton.class);

    install(new FactoryModuleBuilder()
        .implement(BatchAnalyzer.class, BatchAnalyzer.class)
        .build(BatchAnalyzer.BatchAnalyzerFactory.class)
    );

    bind(OplogOperationApplier.class)
        .in(Singleton.class);
  }

  private void bindConfig() {
    bind(ReplCommandLibrary.class)
        .toInstance(config.getReplCommandsLibrary());
    bind(ReplCommandExecutor.class)
        .toInstance(config.getReplCommandsExecutor());
    bind(OplogManager.class)
        .toInstance(config.getReplCoreBundle().getExternalInterface().getOplogManager());
    bind(MongodServer.class)
        .toInstance(config.getMongoDbCoreBundle().getExternalInterface().getMongodServer());
  }

  @Provides
  public OplogBatchChecker createOplogBatchChecker() {
    return new OplogBatchChecker(new ComplexIdOpChecker());
  }

  @Provides
  public OplogBatchFilter createOplogBatchFilter() {
    ToroDbReplicationFilters replFilters = config.getReplCoreBundle()
        .getExternalInterface()
        .getReplicationFilters();
    DatabaseFilter dbFilter = replFilters.getDatabaseFilter();
    NamespaceFilter nsFilter = replFilters.getNamespaceFilter();
    return new OplogBatchFilter(new ByNamespaceOplogOperationFilter(dbFilter, nsFilter));
  }
}
