
package com.torodb.mongodb.repl.guice;

import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapper;
import com.google.common.annotations.Beta;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.repl.MongoClientProvider;
import com.torodb.mongodb.repl.OplogReaderProvider;
import com.torodb.mongodb.repl.RecoveryService;
import com.torodb.mongodb.repl.ReplCoordinator;
import com.torodb.mongodb.repl.SecondaryStateService;
import com.torodb.mongodb.repl.SyncSourceProvider;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.mongodb.repl.impl.MongoOplogReaderProvider;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.FilterProvider;
import com.torodb.mongodb.utils.cloner.CommitHeuristic;

/**
 *
 */
public class MongoDbReplModule extends AbstractModule {

    private static final Logger LOGGER = LogManager.getLogger(MongoDbReplModule.class);

    private final MongoClientConfiguration mongoClientConfiguration;
    private final FilterProvider filterProvider;
    
    public MongoDbReplModule(MongoClientConfiguration mongoClientConfiguration, FilterProvider filterProvider) {
        this.mongoClientConfiguration = mongoClientConfiguration;
        this.filterProvider = filterProvider;
    }

    @Override
    protected void configure() {
        bind(MongoClientProvider.class)
                .toInstance((mongoClientConfiguration) -> new MongoClientWrapper(mongoClientConfiguration));
        bind(OplogReaderProvider.class).to(MongoOplogReaderProvider.class).asEagerSingleton();

        bind(ReplCoordinator.ReplCoordinatorOwnerCallback.class)
                .toInstance(() -> {
                    LOGGER.error("ReplCoord has been stopped");
                }
        );

        install(new FactoryModuleBuilder()
                .implement(SecondaryStateService.class, SecondaryStateService.class)
                .build(SecondaryStateService.SecondaryStateServiceFactory.class)
        );
        install(new FactoryModuleBuilder()
                .implement(RecoveryService.class, RecoveryService.class)
                .build(RecoveryService.RecoveryServiceFactory.class)
        );

        bind(DbCloner.class)
                .annotatedWith(MongoDbRepl.class)
                .toProvider(AkkaDbClonerProvider.class);
//                .toProvider(ConcurrentDbClonerProvider.class);

        bind(CommitHeuristic.class)
                .to(DefaultCommitHeuristic.class)
                .in(Singleton.class);

        bind(Integer.class)
                .annotatedWith(DocsPerTransaction.class)
                .toInstance(1000);

        bind(ThreadFactory.class)
                .annotatedWith(MongoDbRepl.class)
                .toInstance(new ThreadFactoryBuilder()
                        .setNameFormat("repl-unnamed-%d")
                        .build()
                );

        bind(FilterProvider.class).toInstance(filterProvider);
    }


    @Provides @Singleton
    SyncSourceProvider createSyncSourceProvider() {
        if (mongoClientConfiguration != null) {
            return new FollowerSyncSourceProvider(mongoClientConfiguration);
        }
        else {
            return new PrimarySyncSourceProvider();
        }
    }

    private static class DefaultCommitHeuristic implements CommitHeuristic {

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

    @Beta
    private static class FollowerSyncSourceProvider implements SyncSourceProvider {
        private final MongoClientConfiguration syncSource;

        public FollowerSyncSourceProvider(@Nonnull MongoClientConfiguration syncSource) {
            this.syncSource = syncSource;
        }

        @Override
        public MongoClientConfiguration calculateSyncSource(HostAndPort oldSyncSource) {
            return syncSource;
        }

        @Override
        public MongoClientConfiguration getLastUsedSyncSource() {
            return syncSource;
        }

        @Override
        public MongoClientConfiguration getSyncSource(OpTime lastFetchedOpTime) throws
                NoSyncSourceFoundException {
            return syncSource;
        }

    }

    private static class PrimarySyncSourceProvider implements SyncSourceProvider {

        @Override
        public MongoClientConfiguration calculateSyncSource(HostAndPort oldSyncSource) throws
                NoSyncSourceFoundException {
            throw new NoSyncSourceFoundException();
        }

        @Override
        public MongoClientConfiguration getSyncSource(OpTime lastFetchedOpTime) throws
                NoSyncSourceFoundException {
            throw new NoSyncSourceFoundException();
        }

        @Override
        public MongoClientConfiguration getLastUsedSyncSource() {
            return null;
        }
        
    }

}
