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

import com.google.common.collect.ImmutableMap;
import com.google.inject.*;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.backend.BackendServiceImpl;
import com.torodb.core.backend.BackendService;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer.BatchAnalyzerFactory;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodServer;

/**
 *
 * @author gortiz
 */
public abstract class DefaultOplogApplierTest extends AbstractOplogApplierTest {

  @Override
  public Module getMongodSpecificTestModule() {
    return new DefaultMongodModule();
  }

  private static class DefaultMongodModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(ConcurrentOplogBatchExecutor.class)
          .in(Singleton.class);

      bind(AnalyzedOplogBatchExecutor.class)
          .to(ConcurrentOplogBatchExecutor.class)
          .in(Singleton.class);
      expose(AnalyzedOplogBatchExecutor.class);

      bind(ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics.class)
          .in(Singleton.class);
      bind(AnalyzedOplogBatchExecutor.AnalyzedOplogBatchExecutorMetrics.class)
          .to(ConcurrentOplogBatchExecutorMetrics.class);

      bind(ConcurrentOplogBatchExecutor.SubBatchHeuristic.class)
          .toInstance((ConcurrentOplogBatchExecutorMetrics metrics) -> 100);

      install(new FactoryModuleBuilder()
          .implement(BatchAnalyzer.class, BatchAnalyzer.class)
          .build(BatchAnalyzer.BatchAnalyzerFactory.class)
      );
      expose(BatchAnalyzerFactory.class);

      bind(AnalyzedOpReducer.class)
          .toInstance(new AnalyzedOpReducer(true));

      bind(ReplicationFilters.class)
          .toInstance(new ReplicationFilters(ImmutableMap.of(), ImmutableMap.of()));
      expose(ReplicationFilters.class);

      bind(BackendService.class)
          .to(BackendServiceImpl.class)
          .asEagerSingleton();
    }

    @Provides
    TorodServer getMongodServer(TorodBundle bundle) {
      return bundle.getTorodServer();
    }
  }

}
