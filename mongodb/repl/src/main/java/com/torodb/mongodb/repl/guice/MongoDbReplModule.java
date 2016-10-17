
package com.torodb.mongodb.repl.guice;

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapperModule;
import com.google.common.net.HostAndPort;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.core.supervision.Supervisor;
import com.torodb.mongodb.repl.*;
import com.torodb.mongodb.repl.impl.MongoOplogReaderProvider;
import com.torodb.mongodb.repl.impl.ReplicationErrorHandlerImpl;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplier;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplier.BatchLimits;
import com.torodb.mongodb.repl.oplogreplier.DefaultOplogApplierService;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier;
import com.torodb.mongodb.repl.oplogreplier.OplogApplierService;
import com.torodb.mongodb.repl.oplogreplier.analyzed.AnalyzedOpReducer;
import com.torodb.mongodb.repl.oplogreplier.batch.AnalyzedOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.BatchAnalyzer;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor;
import com.torodb.mongodb.repl.oplogreplier.batch.ConcurrentOplogBatchExecutor.ConcurrentOplogBatchExecutorMetrics;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher;
import com.torodb.mongodb.repl.topology.RemoteSeed;
import com.torodb.mongodb.repl.topology.TopologyGuiceModule;
import com.torodb.mongodb.repl.topology.TopologyService;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import java.time.Duration;
import javax.inject.Singleton;

/**
 *
 */
public class MongoDbReplModule extends PrivateModule {

    private final MongodbReplConfig config;

    public MongoDbReplModule(MongodbReplConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        expose(TopologyService.class);
        expose(ReplCoordinator.class);
        expose(OplogManager.class);

        bind(ReplCoordinator.class)
                .in(Singleton.class);
        bind(OplogManager.class)
                .in(Singleton.class);

        install(new MongoClientWrapperModule());

        bind(OplogReaderProvider.class).to(MongoOplogReaderProvider.class).asEagerSingleton();

        install(new FactoryModuleBuilder()
                //To use the old applier that emulates MongoDB
//                .implement(OplogApplierService.class, SequentialOplogApplierService.class)

                //To use the applier service that delegates on a OplogApplier
                .implement(OplogApplierService.class, DefaultOplogApplierService.class)
                .build(OplogApplierService.OplogApplierServiceFactory.class)
        );

        install(new FactoryModuleBuilder()
                .implement(RecoveryService.class, RecoveryService.class)
                .build(RecoveryService.RecoveryServiceFactory.class)
        );

        install(new FactoryModuleBuilder()
                .implement(ContinuousOplogFetcher.class, ContinuousOplogFetcher.class)
                .build(ContinuousOplogFetcher.ContinuousOplogFetcherFactory.class)
        );

        bind(DbCloner.class)
                .annotatedWith(MongoDbRepl.class)
                .toProvider(AkkaDbClonerProvider.class);

        bind(OplogApplier.class)
                .to(DefaultOplogApplier.class)
                .in(Singleton.class);

        bind(DefaultOplogApplier.BatchLimits.class)
                .toInstance(new BatchLimits(1000, Duration.ofSeconds(2)));

        bind(CommitHeuristic.class)
                .to(DefaultCommitHeuristic.class)
                .in(Singleton.class);

        bind(Integer.class)
                .annotatedWith(DocsPerTransaction.class)
                .toInstance(1000);

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
                .toInstance(new AnalyzedOpReducer(false));

        install(new TopologyGuiceModule());

        bind(ReplicationErrorHandler.class)
                .to(ReplicationErrorHandlerImpl.class)
                .in(Singleton.class);

        bind(Supervisor.class)
                .annotatedWith(MongoDbRepl.class)
                .to(ReplicationErrorHandler.class);
        bind(MongodbReplConfig.class)
                .toInstance(config);
    }

    @Provides
    MongoClientConfiguration getMongoClientConf(MongodbReplConfig config) {
        return config.getMongoClientConfiguration();
    }

    @Provides
    ReplicationFilters getReplicationFilters(MongodbReplConfig config) {
        return config.getReplicationFilters();
    }

    @Provides @ReplSetName
    String getReplSetName(MongodbReplConfig config) {
        return config.getReplSetName();
    }

    @Provides @RemoteSeed
    HostAndPort getRemoteSeed(MongodbReplConfig config) {
        return config.getMongoClientConfiguration().getHostAndPort();
    }

    public static class DefaultCommitHeuristic implements CommitHeuristic {

        @Override
        public void notifyDocumentInsertionCommit(int docBatchSize, long millisSpent) {
        }

        @Override
        public int getDocumentsPerCommit() {
            return 1000;
        }

        @Override
        public boolean shouldCommitAfterIndex() {
            return false;
        }
    }
}
