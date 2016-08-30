
package com.torodb.mongodb.repl.guice;

import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.net.SocketFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapper;
import com.google.common.annotations.Beta;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoOptions;
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

    private final HostAndPort syncSource;
    private final MongoClientOptions mongoClientOptions;
    private final MongoCredential mongoCredential;
    private final FilterProvider filterProvider;
    
    public MongoDbReplModule(HostAndPort syncSource, MongoClientOptions mongoClientOptions, MongoCredential mongoCredential, FilterProvider filterProvider) {
        this.syncSource = syncSource;
        this.mongoClientOptions = mongoClientOptions;
        this.mongoCredential = mongoCredential;
        this.filterProvider = filterProvider;
    }

    @Override
    protected void configure() {
        bind(MongoClientProvider.class)
                .toInstance((hostAndPort, mongoClientOptions, mongoCredential) -> new MongoClientWrapper(hostAndPort, mongoClientOptions, mongoCredential));
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
        if (syncSource != null) {
            return new FollowerSyncSourceProvider(syncSource, mongoClientOptions, mongoCredential);
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
        private final HostAndPort syncSource;
        private final MongoClientOptions mongoClientOptions;
        private final MongoCredential mongoCredential;

        public FollowerSyncSourceProvider(@Nonnull HostAndPort syncSource, MongoClientOptions mongoClientOptions, MongoCredential mongoCredential) {
            this.syncSource = syncSource;
            this.mongoClientOptions = mongoClientOptions;
            this.mongoCredential = mongoCredential;
        }

        @Override
        public HostAndPort calculateSyncSource(HostAndPort oldSyncSource) {
            return syncSource;
        }

        @Override
        public HostAndPort getLastUsedSyncSource() {
            return syncSource;
        }

        @Override
        public HostAndPort getSyncSource(OpTime lastFetchedOpTime) throws
                NoSyncSourceFoundException {
            return syncSource;
        }

        @Override
        public MongoClientOptions getMongoClientOptions() {
            return mongoClientOptions;
        }

        @Override
        public MongoCredential getCredential() {
            return mongoCredential;
        }
    }

    private static class PrimarySyncSourceProvider implements SyncSourceProvider {

        @Override
        public HostAndPort calculateSyncSource(HostAndPort oldSyncSource) throws
                NoSyncSourceFoundException {
            throw new NoSyncSourceFoundException();
        }

        @Override
        public HostAndPort getSyncSource(OpTime lastFetchedOpTime) throws
                NoSyncSourceFoundException {
            throw new NoSyncSourceFoundException();
        }

        @Override
        public HostAndPort getLastUsedSyncSource() {
            return null;
        }

        @Override
        public MongoClientOptions getMongoClientOptions() {
            return null;
        }

        @Override
        public MongoCredential getCredential() {
            return null;
        }
    }

}
