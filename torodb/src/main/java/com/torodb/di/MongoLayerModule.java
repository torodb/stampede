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

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.AtomicConnectionIdFactory;
import com.eightkdata.mongowp.mongoserver.callback.RequestProcessor;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.DefaultBuildProperties;
import com.torodb.torod.core.BuildProperties;
import com.torodb.torod.mongodb.*;
import com.torodb.torod.mongodb.annotations.Index;
import com.torodb.torod.mongodb.annotations.Namespaces;
import com.torodb.torod.mongodb.annotations.Standard;
import com.torodb.torod.mongodb.impl.DefaultOpTimeClock;
import com.torodb.torod.mongodb.repl.MongoOplogReader;
import com.torodb.torod.mongodb.repl.OplogReader;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.SyncSourceProvider;
import com.torodb.torod.mongodb.standard.StandardCommandsExecutor;
import com.torodb.torod.mongodb.standard.StandardCommandsLibrary;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.unsafe.ToroQueryCommandProcessor;
import com.torodb.util.mgl.HierarchicalMultipleGranularityLock;
import com.torodb.util.mgl.RootLockedMultipleGranularityLock;
import java.util.concurrent.Executors;
import javax.inject.Provider;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SafeRequestProcessor.SubRequestProcessor.class)
                .annotatedWith(Standard.class)
                .to(ToroStandardSubRequestProcessor.class)
                .asEagerSingleton();

        bind(SafeRequestProcessor.SubRequestProcessor.class)
                .annotatedWith(Index.class)
                .to(ToroIndexSubRequestProcessor.class)
                .asEagerSingleton();

        bind(SafeRequestProcessor.SubRequestProcessor.class)
                .annotatedWith(Namespaces.class)
                .to(ToroNamespacesSubRequestProcessor.class)
                .asEagerSingleton();

        bind(CommandsLibrary.class)
                .annotatedWith(Standard.class)
                .to(StandardCommandsLibrary.class)
                .asEagerSingleton();

        bind(CommandsExecutor.class)
                .annotatedWith(Standard.class)
                .to(StandardCommandsExecutor.class)
                .asEagerSingleton();

        bind(SafeRequestProcessor.class)
                .to(ToroSafeRequestProcessor.class);

        bind(RequestProcessor.class).to(RequestProcessorAdaptor.class);
        bind(ConnectionIdFactory.class).to(AtomicConnectionIdFactory.class).in(Singleton.class);
        bind(ErrorHandler.class).to(ToroErrorHandler.class).in(Singleton.class);
        bind(BuildProperties.class).to(DefaultBuildProperties.class).asEagerSingleton();
        bind(QueryCommandProcessor.class).to(ToroQueryCommandProcessor.class);
        bind(QueryCriteriaTranslator.class).toInstance(new QueryCriteriaTranslator());

        bind(OptimeClock.class).to(DefaultOpTimeClock.class).in(Singleton.class);

        bind(HierarchicalMultipleGranularityLock.class).to(RootLockedMultipleGranularityLock.class).in(Singleton.class);
    }

    @Provides
    ReplCoordinator createCoordinator(Provider<OplogReader> oplogReaderProvider) {
        ReplCoordinator replCoordinator = new ReplCoordinator(
                oplogReaderProvider,
                Executors.newCachedThreadPool()
        );
        replCoordinator.startAsync();

        return replCoordinator;
    }

    @Provides
    OplogReader createOplogReader(SyncSourceProvider ssp) {
        return new MongoOplogReader(ssp);
    }

    @Provides @Singleton
    SyncSourceProvider createSyncSourceProvider() {
        return new MySyncSourceProvider();
    }

    private static class MySyncSourceProvider implements SyncSourceProvider {
        private final HostAndPort syncSource
                    = HostAndPort.fromParts("127.0.0.1", 27020);

        @Override
        public HostAndPort calculateSyncSource(HostAndPort oldSyncSource) {
            return syncSource;
        }

        @Override
        public HostAndPort getLastUsedSyncSource() {
            return syncSource;
        }
    }

}
