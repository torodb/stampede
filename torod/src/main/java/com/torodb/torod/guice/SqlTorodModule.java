
package com.torodb.torod.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.annotations.UseThreads;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.sql.SqlTorodServer;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.AkkaInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline;

/**
 *
 */
public class SqlTorodModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SqlTorodServer.class)
                .in(Singleton.class);

        bind(TorodServer.class)
                .to(SqlTorodServer.class);

        install(new FactoryModuleBuilder()
                .implement(InsertPipeline.class, SameThreadInsertPipeline.class)
                .build(InsertPipelineFactory.class)
        );
    }
    
    @Provides @Singleton @UseThreads
    InsertPipelineFactory createConcurrentPipelineFactory(ConcurrentToolsFactory concurrentToolsFactory,
            BackendTransactionJobFactory backendTransactionJobFactory) {

        return new AkkaInsertPipelineFactory(concurrentToolsFactory, backendTransactionJobFactory, 100);
    }

}
