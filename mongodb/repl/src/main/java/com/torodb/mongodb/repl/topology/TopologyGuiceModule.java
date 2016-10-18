/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with repl. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.topology;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.mongodb.repl.SyncSourceProvider;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import javax.inject.Singleton;

/**
 *
 */
public class TopologyGuiceModule extends PrivateModule {

    @Override
    protected void configure() {

        bind(HeartbeatNetworkHandler.class)
                .to(MongoClientHeartbeatNetworkHandler.class)
                .in(Singleton.class);

        bind(SyncSourceProvider.class)
                .to(RetrierTopologySyncSourceProvider.class)
                .in(Singleton.class);
        expose(SyncSourceProvider.class);

        bind(TopologyErrorHandler.class)
                .to(DefaultTopologyErrorHandler.class)
                .in(Singleton.class);
    }

    @Provides @Singleton @Exposed
    public TopologyService createTopologyService(ThreadFactory threadFactory,
            TopologyHeartbeatHandler heartbeatHandler, TopologyExecutor executor,
            Clock clock) {
        return new TopologyService(heartbeatHandler, threadFactory, executor, clock);
    }

    @Provides @Singleton
    TopologyExecutor createTopologyExecutor(
            ConcurrentToolsFactory concurrentToolsFactory) {
        //TODO: Being able to configure max sync source lag and replication delay
        return new TopologyExecutor(concurrentToolsFactory, Duration.ofMinutes(1),
                Duration.ZERO);
    }
}