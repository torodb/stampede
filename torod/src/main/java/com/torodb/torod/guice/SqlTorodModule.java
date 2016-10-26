
package com.torodb.torod.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.annotations.UseThreads;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.AkkaInsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.sql.SqlTorodServer;

/**
 *
 */
public class SqlTorodModule extends PrivateModule {

    @Override
    protected void configure() {
        install(new FactoryModuleBuilder()
                .implement(InsertPipeline.class, SameThreadInsertPipeline.class)
                .build(InsertPipelineFactory.class)
        );

        install(new FactoryModuleBuilder()
                .implement(TorodBundle.class, TorodBundle.class)
                .build(TorodBundleFactory.class)
        );
        expose(TorodBundleFactory.class);

        bind(TorodServer.class)
                .to(SqlTorodServer.class)
                .in(Singleton.class);
    }
    
    @Provides @Singleton @UseThreads
    InsertPipelineFactory createConcurrentPipelineFactory(ConcurrentToolsFactory concurrentToolsFactory,
            BackendTransactionJobFactory backendTransactionJobFactory) {

        return new AkkaInsertPipelineFactory(concurrentToolsFactory, backendTransactionJobFactory, 100);
    }

}
