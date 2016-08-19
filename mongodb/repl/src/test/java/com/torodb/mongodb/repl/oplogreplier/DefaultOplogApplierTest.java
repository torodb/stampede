/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.mongodb.repl.oplogreplier;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics;

/**
 *
 * @author gortiz
 */
public class DefaultOplogApplierTest extends AbstractOplogApplierTest {

    Module module = new AkkaOplogApplierTestModule();

    @Override
    public Module getSpecificTestModule() {
        return module;
    }

    private static class AkkaOplogApplierTestModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(ConcurrentOplogBatchExecutor.class)
                    .in(Singleton.class);

            bind(AnalyzedOplogBatchExecutor.class)
                    .to(ConcurrentOplogBatchExecutor.class);

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
            bind(AnalyzedOpReducer.class)
                    .toInstance(new AnalyzedOpReducer(true));

        }
    }


}
