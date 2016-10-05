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

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.net.HostAndPort;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This is the class used by external classes to talk with the topology layer.
 */
@Singleton
public class TopologyInterface {
    private final Clock clock;
    private final TopologyExecutor executor;

    @Inject
    public TopologyInterface(Clock clock, TopologyExecutor executor) {
        this.clock = clock;
        this.executor = executor;
    }
    
    public CompletableFuture<Empty> initiate(ReplicaSetConfig rsConfig) {
        return executor.onAnyVersion().mapAsync(coord -> {
            coord.updateConfig(rsConfig, clock.instant());
            return Empty.getInstance();
        });
    }
    
    public CompletableFuture<Optional<HostAndPort>> getLastUsedSyncSource() {
        return executor.onAnyVersion().mapAsync(TopologyCoordinator::getSyncSourceAddress);
    }

    public CompletableFuture<Optional<HostAndPort>> chooseNewSyncSource(Optional<OpTime> lastFetchedOpTime) {
        return executor.onAnyVersion().mapAsync(coord -> coord.chooseNewSyncSource(clock.instant(), lastFetchedOpTime));
    }

}
