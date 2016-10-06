
package com.torodb.mongodb.repl.guice;

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapperModule;
import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
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
import com.torodb.mongodb.repl.topology.TopologyGuiceModule;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;
import java.time.Duration;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MongoDbReplModule extends AbstractModule {

    private static final Logger LOGGER = LogManager.getLogger(MongoDbReplModule.class);

    private final MongoClientConfiguration mongoClientConfiguration;
    private final ReplicationFilters replicationFilters;
    private final String replSetName;
    
    public MongoDbReplModule(MongoClientConfiguration mongoClientConfiguration, 
            ReplicationFilters replicationFilters, String replSetName) {
        this.mongoClientConfiguration = mongoClientConfiguration;
        this.replicationFilters = replicationFilters;
        this.replSetName = replSetName;
    }

    @Override
    protected void configure() {
        //Binds necessary to instanciate mongodb clients
        bind(MongoClientConfiguration.class)
                .toInstance(mongoClientConfiguration);
        install(new MongoClientWrapperModule());

        bind(OplogReaderProvider.class).to(MongoOplogReaderProvider.class).asEagerSingleton();

        bind(ReplCoordinator.ReplCoordinatorOwnerCallback.class)
                .toInstance(() -> {
                    LOGGER.error("ReplCoord has been stopped");
                }
        );

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

        bind(ReplicationFilters.class).toInstance(replicationFilters);

        bind(String.class).annotatedWith(ReplSetName.class).toInstance(replSetName);

        install(new TopologyGuiceModule(mongoClientConfiguration));

        bind(ReplicationErrorHandler.class)
                .to(ReplicationErrorHandlerImpl.class)
                .in(Singleton.class);
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
