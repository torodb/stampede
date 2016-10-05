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

import com.eightkdata.mongowp.Status;
import com.google.common.net.HostAndPort;
import com.torodb.common.util.ThreadFactoryIdleService;
import java.util.concurrent.ThreadFactory;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class TopologyService extends ThreadFactoryIdleService {
    private static final Logger LOGGER = LogManager.getLogger(TopologyService.class);

    private final HostAndPort seed;
    private final TopologyHeartbeatHandler heartbeatHandler;

    @Inject
    public TopologyService(HostAndPort seed,
            TopologyHeartbeatHandler heartbeatHandler,
            ThreadFactory threadFactory) {
        super(threadFactory);
        this.seed = seed;
        this.heartbeatHandler = heartbeatHandler;
    }

    @Override
    protected void startUp() throws Exception {
        boolean finished = false;
        LOGGER.debug("Starting topology service");
        while (!finished) {
            finished = heartbeatHandler.start(seed)
                    .handle(this::checkHeartbeatStarted)
                    .join();
            if (!finished) {
                LOGGER.debug("Trying to start heartbeats in 1 second");
                Thread.sleep(1000);
            }
        }
        LOGGER.info("Topology service started");
    }

    private boolean checkHeartbeatStarted(Status<?> status, Throwable t) {
        if (t == null) {
            if (status.isOk()) {
                LOGGER.trace("Heartbeat started correctly");
                return true;
            }
            else {
                LOGGER.debug("Heartbeat start failed: {}", status);
                return false;
            }
        } else {
            LOGGER.debug("Heartbeat start failed", t);
            return false;
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Topology service shutted down");
    }

    

}
