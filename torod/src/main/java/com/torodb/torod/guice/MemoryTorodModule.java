
package com.torodb.torod.guice;

import com.google.common.util.concurrent.AbstractService;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.dsl.backend.BackendTransactionJobFactory;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.TorodServer;
import com.torodb.torod.impl.memory.MemoryTorodServer;
import com.torodb.torod.pipeline.InsertPipeline;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import com.torodb.torod.pipeline.impl.SameThreadInsertPipeline;
import javax.inject.Inject;

/**
 *
 */
public class MemoryTorodModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(MemoryTorodServer.class)
                .in(Singleton.class);
        
        bind(TorodServer.class)
                .to(MemoryTorodServer.class);

        install(new FactoryModuleBuilder()
                .implement(TorodBundle.class, TorodBundle.class)
                .build(TorodBundleFactory.class)
        );
        expose(TorodBundleFactory.class);

        bind(InsertPipelineFactory.class)
                .to(MemoryInsertPipelineFactory.class)
                .in(Singleton.class);
    }

    private static class MemoryInsertPipelineFactory extends AbstractService
            implements InsertPipelineFactory {

        private final BackendTransactionJobFactory backendTransactionJobFactory;

        @Inject
        public MemoryInsertPipelineFactory(
                BackendTransactionJobFactory backendTransactionJobFactory) {
            this.backendTransactionJobFactory = backendTransactionJobFactory;
        }

        @Override
        protected void doStart() {
            notifyStarted();
        }

        @Override
        protected void doStop() {
            notifyStopped();
        }

        @Override
        public InsertPipeline createInsertPipeline(
                D2RTranslatorFactory translatorFactory, MetaDatabase metaDb,
                MutableMetaCollection mutableMetaCollection,
                WriteBackendTransaction backendConnection, boolean concurrent) {
            return new SameThreadInsertPipeline(translatorFactory, metaDb,
                    mutableMetaCollection, backendConnection, 
                    backendTransactionJobFactory);
        }

    }

}
