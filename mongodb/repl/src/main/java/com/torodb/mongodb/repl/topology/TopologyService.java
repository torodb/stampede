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
import com.torodb.core.services.IdleTorodbService;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class TopologyService extends IdleTorodbService {
    private static final Logger LOGGER = LogManager.getLogger(TopologyService.class);

    private final TopologyHeartbeatHandler heartbeatHandler;
    private final TopologyExecutor executor;
    private final Clock clock;

    @Inject
    public TopologyService(TopologyHeartbeatHandler heartbeatHandler,
            ThreadFactory threadFactory, TopologyExecutor executor,
            Clock clock) {
        super(threadFactory);
        this.heartbeatHandler = heartbeatHandler;
        this.executor = executor;
        this.clock = clock;
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

    @Override
    protected void startUp() throws Exception {
        LOGGER.debug("Starting topology service");

        heartbeatHandler.startAsync();
        heartbeatHandler.awaitRunning();

        boolean topologyReady;
        int attempts = 0;
        do {
            topologyReady = calculateTopologyReady();
            if (!topologyReady) {
                LOGGER.debug("Waiting until topology is ready");
                Thread.sleep(1000);
            }
            attempts++;
        }
        while (!topologyReady && attempts < 30);
        if (!topologyReady) {
            throw new RuntimeException("Topology was not able to be ready "
                    + "after " + attempts + " attempts");
        }
        

        LOGGER.info("Topology service started");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Topology service shutted down");

        heartbeatHandler.shutDown();
        heartbeatHandler.awaitTerminated();
    }

    private boolean isTopologyReady(TopologyCoordinator coord) {
        return coord.chooseNewSyncSource(clock.instant(), Optional.empty()).isPresent();
    }

    private boolean calculateTopologyReady() {
        try {
            return executor.onAnyVersion()
                    .mapAsync(this::isTopologyReady)
                    .join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            throw new RuntimeException("Topology startup failed before it "
                    + "was ready to accept requests", cause);
        }
    }
    

}