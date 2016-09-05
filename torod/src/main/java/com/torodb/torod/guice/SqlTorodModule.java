
package com.torodb.torod.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.sql.SqlTorodServer;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.AkkaInsertPipelineFactory;

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
    }
    
    @Provides @Singleton
    InsertPipelineFactory createPipelineFactory(ConcurrentToolsFactory concurrentToolsFactory,
            BackendTransactionJobFactory backendTransactionJobFactory) {

        return new AkkaInsertPipelineFactory(concurrentToolsFactory, backendTransactionJobFactory, 100);
    }

}
