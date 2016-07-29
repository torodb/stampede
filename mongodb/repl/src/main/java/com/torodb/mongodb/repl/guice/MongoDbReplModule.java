
package com.torodb.mongodb.repl.guice;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapper;
import com.google.common.annotations.Beta;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.torodb.mongodb.repl.*;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.mongodb.repl.impl.MongoOplogReaderProvider;
import com.torodb.mongodb.utils.AkkaDbCloner;
import com.torodb.mongodb.utils.AkkaDbCloner.CommitHeuristic;
import com.torodb.mongodb.utils.DbCloner;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MongoDbReplModule extends AbstractModule {

    private static final Logger LOGGER = LogManager.getLogger(MongoDbReplModule.class);

    private final HostAndPort syncSource;
    public MongoDbReplModule(HostAndPort syncSource) {
        this.syncSource = syncSource;
    }

    @Override
    protected void configure() {
        bind(MongoClientProvider.class)
                .toInstance((hostAndPort) -> new MongoClientWrapper(hostAndPort));
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

    }


    @Provides @Singleton
    SyncSourceProvider createSyncSourceProvider() {
        if (syncSource != null) {
            return new FollowerSyncSourceProvider(syncSource);
        }
        else {
            return new PrimarySyncSourceProvider();
        }
    }

    @Provides @Singleton @MongoDbRepl
    DbCloner createReplDbCloner(@MongoDbRepl ExecutorService executorService) {
        final int parallelLevel = 5;
        final int insertBatch = 10000;
        final int docsPerCommit = 1000;
        LOGGER.info("Using AkkaDbCloner with: {parallelLevel: {}, insertBatch: {}, docsPerCommit: {}}",
                parallelLevel, insertBatch, docsPerCommit);
        return new AkkaDbCloner(
                executorService,
                parallelLevel - 1,
                parallelLevel * insertBatch,
                insertBatch,
                new CommitHeuristic() {
                    @Override
                    public void notifyDocumentInsertionCommit(int docBatchSize, long millisSpent) {
                    }

                    @Override
                    public int getDocumentsPerCommit() {
                        return docsPerCommit;
                    }

                    @Override
                    public boolean shouldCommitAfterIndex() {
                        return true;
                    }
                },
                Clock.systemDefaultZone()
        );
    }

    @Beta
    private static class FollowerSyncSourceProvider implements SyncSourceProvider {
        private final HostAndPort syncSource;

        public FollowerSyncSourceProvider(@Nonnull HostAndPort syncSource) {
            this.syncSource = syncSource;
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

    }

}
