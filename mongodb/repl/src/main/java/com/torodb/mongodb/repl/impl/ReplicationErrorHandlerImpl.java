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

package com.torodb.mongodb.repl.impl;

import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.core.services.TorodbService;
import com.torodb.mongodb.repl.ReplCoordinator;
import com.torodb.mongodb.repl.ReplicationErrorHandler;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class ReplicationErrorHandlerImpl implements ReplicationErrorHandler {
    private static final Logger LOGGER = LogManager.getLogger(ReplicationErrorHandlerImpl.class);
    private final Provider<ReplCoordinator> replCoordProvider;

    @Inject
    public ReplicationErrorHandlerImpl(Provider<ReplCoordinator> replCoordProvider) {
        this.replCoordProvider = replCoordProvider;
    }

    @Override
    public void onTopologyError(Throwable t) {
        ReplCoordinator replCoord = replCoordProvider.get();
        if (replCoord.isRunning()) {
            LOGGER.error("Fatal error on topology layer. Stopping replication layer");
            replCoord.stopAsync();
        } else {
            LOGGER.warn("Found a new error while replication is already stopped");
        }
    }

    @Override
    public SupervisorDecision onError(TorodbService supervised, Throwable error) {
        onTopologyError(error);
        return SupervisorDecision.STOP;
    }

}
