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
 * along with mongodb-layer. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.mongodb.repl.topology;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetHeartbeatReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.repl.guice.ReplSetName;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



/**
 *
 */
@Singleton
class TopologyHeartbeatHandler {
    private static final Logger LOGGER = LogManager.getLogger(TopologyHeartbeatHandler.class);
    private static final Duration POST_ERROR_HB_DELAY = Duration.ofSeconds(2);

    private final Clock clock;
    private final String replSetName;
    private final HeartbeatNetworkHandler networkHandler;
    private final TopologyExecutor executor;
    private final TopologyErrorHandler errorHandler;

    @Inject
    public TopologyHeartbeatHandler(Clock clock, @ReplSetName String replSetName,
            HeartbeatNetworkHandler heartbeatSender, TopologyExecutor executor,
            TopologyErrorHandler errorHandler) {
        this.clock = clock;
        this.replSetName = replSetName;
        this.networkHandler = heartbeatSender;
        this.executor = executor;
        this.errorHandler = errorHandler;
    }

    public CompletableFuture<Status<?>> start(HostAndPort seed) {
        return executor.onCurrentVersion().andThenApplyAsync(
                networkHandler.askForConfig(
                        new RemoteCommandRequest<>(seed, "admin", Empty.getInstance())
                ),
                (coord, remoteConfig) -> {
                    if (remoteConfig.isOk()) {
                        coord.addVersionChangeListener((coord2, old) -> startHeartbeats(coord2));
                        updateConfig(coord, remoteConfig.getCommandReply().get());
                    }
                    return remoteConfig.asStatus();
                }
        );
    }

    @GuardedBy("executor")
    private void startHeartbeats(TopologyCoordinator coord) {
        coord.getRsConfig().getMembers().stream()
                .forEach(member -> scheduleHeartbeatToTarget(member.getHostAndPort(), Duration.ZERO));
    }

    private CompletableFuture<?> scheduleHeartbeatToTarget(final HostAndPort target, Duration delay) {
        LOGGER.debug("Scheduling heartbeat to {} in {}", target, delay);

        return executor.onCurrentVersion()
                .scheduleOnce((coord) -> doHeartbeat(coord, target), delay);
    }

    @GuardedBy("executor")
    private void doHeartbeat(final TopologyCoordinator coord, final HostAndPort target) {

        RemoteCommandRequest<ReplSetHeartbeatArgument> request = coord.prepareHeartbeatRequest(
                clock.instant(), replSetName, target);

        CompletableFuture<RemoteCommandResponse<ReplSetHeartbeatReply>> hbHandle = networkHandler
                .sendHeartbeat(request);

        hbHandle.exceptionally(t -> {
            LOGGER.trace("Error while sending a heartbeat to " + target, t);
            if (errorHandler.sendHeartbeatError(t)) {
                LOGGER.trace("Rescheduling a new heartbeat to {} on {}", target, POST_ERROR_HB_DELAY);
                scheduleHeartbeatToTarget(target, POST_ERROR_HB_DELAY);
            }
            return null;
        });


        CompletableFuture<?> executeResponseFuture = executor.onCurrentVersion().andThenAcceptAsync(
                hbHandle,
                (coord2, response) -> handleHeartbeatResponse(
                                coord2, target, request.getCmdObj(), response)
                );

        executeResponseFuture.exceptionally(t -> {
            if (!(t instanceof CancellationException)) {
                LOGGER.trace("Error while handling a heartbeat response from " + target, t);
                if (errorHandler.reciveHeartbeatError(t)) {
                    LOGGER.trace("Rescheduling a new heartbeat to {} on {}", target, POST_ERROR_HB_DELAY);
                    scheduleHeartbeatToTarget(target, POST_ERROR_HB_DELAY);
                }
            }
            return null;
        });
    }

    @GuardedBy("executor")
    private void handleHeartbeatResponse(TopologyCoordinator coord, HostAndPort target, ReplSetHeartbeatArgument request,
            RemoteCommandResponse<ReplSetHeartbeatReply> response) {
        boolean isUnauthorized = (response.getErrorCode() == ErrorCode.UNAUTHORIZED) ||
            (response.getErrorCode() == ErrorCode.AUTHENTICATION_FAILED);
        Instant now = clock.instant();
        Duration networkTime = Duration.ZERO;

        if (response.isOk()) {
            networkTime = response.getNetworkTime();
        } else {
            LOGGER.info("Error in heartbeat request to {}; {}", target, response.asStatus());
            if (response.getBson() != null) {
                LOGGER.info("heartbeat response: ", response.getBson());
            }

            if (isUnauthorized) {
                networkTime = response.getNetworkTime();
            }
        }

        HeartbeatResponseAction action = coord.processHeartbeatResponse(now, networkTime, target,
                response);

        ReplSetHeartbeatReply hbReply = response.getCommandReply().orElse(null);
        assert hbReply != null || !response.isOk() :
                "Recived a null hbReply when the request didn't fail";

        scheduleHeartbeatToTarget(target, action.getNextHeartbeatDelay());

        handleHeartbeatResponseAction(coord, action, hbReply, response.getErrorCode());

    }

    @GuardedBy("executor")
    private void handleHeartbeatResponseAction(TopologyCoordinator coord, HeartbeatResponseAction action,
            @Nullable ReplSetHeartbeatReply reply, ErrorCode responseStatus)
            throws UnsupportedHeartbeatResponseActionException{
        switch (action.getAction()) {
            case NO_ACTION:
                break;
            case RECONFIG:
                assert reply != null;
                assert reply.getConfig().isPresent();
                updateConfig(coord, reply.getConfig().get());
                break;
            case START_ELECTION:
            case STEP_DOWN_SELF:
            case STEP_DOWN_REMOTE_PRIMARY: 
                throw new UnsupportedHeartbeatResponseActionException(action, reply);
            default:
                LOGGER.error("Illegal heartbeat response action code {}", action.getAction());
                throw new AssertionError();
        }
    }

    private void updateConfig(TopologyCoordinator coord, ReplicaSetConfig config) {
        validateConfig(coord, config);
        coord.updateConfig(config, clock.instant());
    }

    @GuardedBy("executor")
    private void validateConfig(TopologyCoordinator coord, ReplicaSetConfig config) {
        LOGGER.debug("Accepting the new replica set config (version is {}) without validating it first (not supported yet)", config.getConfigVersion());
    }

    private static class UnsupportedHeartbeatResponseActionException extends RuntimeException {

        private static final long serialVersionUID = 8879568483145061898L;
        private final HeartbeatResponseAction action;
        private final @Nullable ReplSetHeartbeatReply reply;

        public UnsupportedHeartbeatResponseActionException(HeartbeatResponseAction action, ReplSetHeartbeatReply reply) {
            super("Heartbeat action " + action.getAction() + " is not supported");
            this.action = action;
            this.reply = reply;
        }

        public HeartbeatResponseAction getAction() {
            return action;
        }

        public ReplSetHeartbeatReply getReply() {
            return reply;
        }
    }
}
