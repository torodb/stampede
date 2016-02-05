/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.di;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.netty.NettyStringReader;
import com.eightkdata.mongowp.bson.netty.PooledNettyStringReader;
import com.eightkdata.mongowp.bson.netty.pool.OnlyLikelyStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.ShortStringPoolPolicy;
import com.eightkdata.mongowp.bson.netty.pool.StringPool;
import com.eightkdata.mongowp.bson.netty.pool.WeakMapStringPool;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.client.wrapper.MongoClientWrapper;
import com.eightkdata.mongowp.server.api.*;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor;
import com.eightkdata.mongowp.server.api.impl.AtomicConnectionIdFactory;
import com.eightkdata.mongowp.server.callback.RequestProcessor;
import com.eightkdata.mongowp.server.wp.DefaultRequestIdGenerator;
import com.eightkdata.mongowp.server.wp.RequestIdGenerator;
import com.google.common.annotations.Beta;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.DefaultBuildProperties;
import com.torodb.Shutdowner;
import com.torodb.config.model.Config;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.ToroErrorHandler;
import com.torodb.torod.mongodb.annotations.External;
import com.torodb.torod.mongodb.annotations.Local;
import com.torodb.torod.mongodb.commands.ToroCommandsExecutorProvider;
import com.torodb.torod.mongodb.commands.ToroCommandsLibraryProvider;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction.AlwaysTransactionalWriteFailMode;
import com.torodb.torod.mongodb.crp.Index;
import com.torodb.torod.mongodb.crp.MetaCollectionRequestProcessor;
import com.torodb.torod.mongodb.crp.Namespaces;
import com.torodb.torod.mongodb.impl.DefaultOpTimeClock;
import com.torodb.torod.mongodb.impl.LocalMongoClient;
import com.torodb.torod.mongodb.meta.IndexesMetaCollection;
import com.torodb.torod.mongodb.meta.NamespacesMetaCollection;
import com.torodb.torod.mongodb.repl.OplogManager;
import com.torodb.torod.mongodb.repl.OplogReaderProvider;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.ReplCoordinator.ReplCoordinatorOwnerCallback;
import com.torodb.torod.mongodb.repl.SyncSourceProvider;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.torod.mongodb.repl.impl.MongoOplogReaderProvider;
import com.torodb.torod.mongodb.srp.DatabaseCheckSafeRequestProcessor;
import com.torodb.torod.mongodb.srp.DatabaseIgnoreSafeRequestProcessor;
import com.torodb.torod.mongodb.srp.ToroSafeRequestProcessor;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.unsafe.ToroQueryCommandProcessor;
import com.torodb.torod.mongodb.utils.DBCloner;
import com.torodb.torod.mongodb.utils.MongoClientProvider;
import com.torodb.torod.mongodb.utils.OplogOperationApplier;
import javax.annotation.Nonnull;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

    private final HostAndPort syncSource;

    public MongoLayerModule(Config config) {
    	if (config.getProtocol().getMongo().getReplication() != null &&
    			!config.getProtocol().getMongo().getReplication().isEmpty() &&
    			config.getProtocol().getMongo().getReplication().get(0).getSyncSource() != null) {
    		this.syncSource = HostAndPort.fromString(config.getProtocol().getMongo().getReplication().get(0).getSyncSource());
    	} else {
    		this.syncSource = null;
    	}
    }

    @Override
    protected void configure() {
        bind(CommandsLibrary.class)
                .toProvider(ToroCommandsLibraryProvider.class)
                .asEagerSingleton();

        bind(CommandsExecutor.class)
                .toProvider(ToroCommandsExecutorProvider.class)
                .asEagerSingleton();

        bind(ToroSafeRequestProcessor.class);
        bind(DatabaseIgnoreSafeRequestProcessor.class);
        bind(SafeRequestProcessor.class)
                .annotatedWith(Local.class)
                .to(DatabaseIgnoreSafeRequestProcessor.class);
        bind(DatabaseCheckSafeRequestProcessor.class);
        bind(SafeRequestProcessor.class)
                .annotatedWith(External.class)
                .to(DatabaseCheckSafeRequestProcessor.class);

        bind(ConnectionIdFactory.class).to(AtomicConnectionIdFactory.class).in(Singleton.class);
        bind(ErrorHandler.class).to(ToroErrorHandler.class).in(Singleton.class);
        bind(BuildProperties.class).to(DefaultBuildProperties.class).asEagerSingleton();
        bind(QueryCommandProcessor.class).to(ToroQueryCommandProcessor.class);
        bind(QueryCriteriaTranslator.class).toInstance(new QueryCriteriaTranslator());

        bind(OptimeClock.class).to(DefaultOpTimeClock.class).in(Singleton.class);

        bind(OplogReaderProvider.class).to(MongoOplogReaderProvider.class).asEagerSingleton();
        bind(OplogOperationApplier.class);
        bind(LocalMongoClient.class);
        bind(DBCloner.class);
        bind(MongoClientProvider.class)
                .toInstance(new WrapperMongoClientProvider());

        bind(ReplCoordinator.class);

        bind(WriteConcernToWriteFailModeFunction.class).to(AlwaysTransactionalWriteFailMode.class);

        bind(Shutdowner.class).in(Singleton.class);
        bind(ReplCoordinatorOwnerCallback.class).to(Shutdowner.class);

        bind(OplogManager.class);

        bind(StringPool.class)
//                .toInstance(new InternStringPool(
//                        OnlyLikelyStringPoolPolicy.getInstance()
//                                .or(new ShortStringPoolPolicy(5))
//                ));

//                .toInstance(new GuavaStringPool(
//                        OnlyLikelyStringPoolPolicy.getInstance()
//                                .or(new ShortStringPoolPolicy(5)),
//                        CacheBuilder.newBuilder().maximumSize(100_000)
//                ));
                .toInstance(new WeakMapStringPool(
                        OnlyLikelyStringPoolPolicy.getInstance()
                                .or(new ShortStringPoolPolicy(5))
                ));

        bind(NettyStringReader.class)
                .to(PooledNettyStringReader.class)
                .in(Singleton.class);
        bind(RequestIdGenerator.class)
                .to(DefaultRequestIdGenerator.class)
                .asEagerSingleton();
    }

    @Provides @Singleton
    RequestProcessor createRequestProcessor(
            ConnectionIdFactory connectionIdFactory,
            @External SafeRequestProcessor safeRequestProcessor,
            ErrorHandler errorHandler) {
        return new RequestProcessorAdaptor(connectionIdFactory, safeRequestProcessor, errorHandler);
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

    @Provides @Index @Singleton
    MetaCollectionRequestProcessor createIndexMetaCollectionRequestProcessor(
            IndexesMetaCollection indexMetaCollection,
            QueryCriteriaTranslator qct,
            OptimeClock optimeClock) {
        return new MetaCollectionRequestProcessor(indexMetaCollection, qct, optimeClock);
    }

    @Provides @Namespaces @Singleton
    MetaCollectionRequestProcessor createNamespacesMetaCollectionRequestProcessor(
            NamespacesMetaCollection indexMetaCollection,
            QueryCriteriaTranslator qct,
            OptimeClock optimeClock) {
        return new MetaCollectionRequestProcessor(indexMetaCollection, qct, optimeClock);
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

    private static class WrapperMongoClientProvider implements MongoClientProvider {

        @Override
        public MongoClient getClient(HostAndPort hostAndPort) throws UnreachableMongoServerException {
            return new MongoClientWrapper(hostAndPort);
        }

    }
}
