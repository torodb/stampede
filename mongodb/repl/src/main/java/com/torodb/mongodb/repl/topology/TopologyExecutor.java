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

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.google.common.util.concurrent.*;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.torodb.common.util.ListeningFutureToCompletableFuture.toCompletableFuture;

/**
 * This class wrappes the single thread {@link ListeningScheduledExecutorService} that controls the
 * access to {@link TopologyCoordinator} methods.
 */
public class TopologyExecutor {

    private static final Logger LOGGER = LogManager.getLogger(TopologyExecutor.class);
    private final TopologyCoordinator coord;
    private final ListeningScheduledExecutorService executor;
    private final OnAnyVersion onAnyVersion;
    private final OnCurrentVersion onCurrentVersion;
    private volatile int version;

    public TopologyExecutor(ConcurrentToolsFactory concurrentToolsFactory,
            Duration maxSyncSourceLag, Duration slaveDelay) {
        this.executor = MoreExecutors.listeningDecorator(
                concurrentToolsFactory.createScheduledExecutorServiceWithMaxThreads("topology-executor", 1)
        );
        this.coord = new TopologyCoordinator(maxSyncSourceLag, slaveDelay);
        this.coord.addVersionChangeListener(this::onVersionChange);
        this.onAnyVersion = new OnAnyVersion(executor, coord);
        this.onCurrentVersion = new OnCurrentVersion(executor, coord, this::getVersion);
    }

    private int getVersion() {
        return version;
    }

    private void onVersionChange(TopologyCoordinator coord, ReplicaSetConfig oldConfig) {
        version = coord.getRsConfig().getConfigVersion();
    }

    VersionExecutor onAnyVersion() {
        return onAnyVersion;
    }

    VersionExecutor onCurrentVersion() {
        return onCurrentVersion;
    }

    void onVersionChange(VersionChangeListener callback) {
        CompletableFuture.runAsync(() -> coord.addVersionChangeListener(callback), executor).join();
    }

    interface VersionExecutor {

        public abstract CompletableFuture<?> consumeAsync(Consumer<TopologyCoordinator> callback);

        public abstract <R> CompletableFuture<R> mapAsync(Function<TopologyCoordinator, R> callback);

        /**
         * Schedules a callback to be executed once a given delay elapses.
         *
         * @param callback
         * @param delay    the delay on which the task will be executed. Sub-millisecond part will
         *                 be ignored
         * @return
         */
        public abstract CompletableFuture<?> scheduleOnce(Consumer<TopologyCoordinator> callback, Duration delay);

        public abstract <E> CompletableFuture<?> andThenConsumeAsync(CompletionStage<E> stage, BiConsumer<TopologyCoordinator, E> consumer);
    }

    private static class OnAnyVersion implements VersionExecutor {
        private final ListeningScheduledExecutorService executor;
        private final TopologyCoordinator coord;

        public OnAnyVersion(ListeningScheduledExecutorService executor, TopologyCoordinator coord) {
            this.executor = executor;
            this.coord = coord;
        }

        @Override
        public CompletableFuture<?> consumeAsync(Consumer<TopologyCoordinator> callback) {
            return CompletableFuture.runAsync(() -> callback.accept(coord), executor);
        }

        @Override
        public <R> CompletableFuture<R> mapAsync(Function<TopologyCoordinator, R> callback) {
            return CompletableFuture.supplyAsync(() -> callback.apply(coord), executor);
        }

        @Override
        public CompletableFuture<?> scheduleOnce(Consumer<TopologyCoordinator> callback, Duration delay) {
            ListenableScheduledFuture<?> listeningFut = executor.schedule(
                    () -> callback.accept(coord), delay.toMillis(), TimeUnit.MILLISECONDS);
            return toCompletableFuture(listeningFut);
        }

        @Override
        public <E> CompletableFuture<?> andThenConsumeAsync(CompletionStage<E> stage, BiConsumer<TopologyCoordinator, E> consumer) {
            return stage.thenAcceptAsync(e -> consumer.accept(coord, e), executor).toCompletableFuture();
        }

    }

    private static class OnCurrentVersion implements VersionExecutor {
        private final ListeningScheduledExecutorService executor;
        private final TopologyCoordinator coord;
        private final IntSupplier versionSupplier;

        public OnCurrentVersion(ListeningScheduledExecutorService executor, TopologyCoordinator coord, IntSupplier versionSupplier) {
            this.executor = executor;
            this.coord = coord;
            this.versionSupplier = versionSupplier;
        }

        private void checkVersion(int originalVersion) {
            int currentVersion = versionSupplier.getAsInt();
            if (originalVersion != currentVersion) {
                throw new CancellationException("Replication configuration changed from "
                        + originalVersion + " to " + currentVersion + " since the task was "
                        + "scheduled");
            }
        }

        @Override
        public CompletableFuture<?> consumeAsync(Consumer<TopologyCoordinator> callback) {
            final int originalVersion = versionSupplier.getAsInt();
            return CompletableFuture.runAsync(() -> {
                checkVersion(originalVersion);
                callback.accept(coord);
            }, executor);
        }

        @Override
        public <R> CompletableFuture<R> mapAsync(Function<TopologyCoordinator, R> callback) {
            final int originalVersion = versionSupplier.getAsInt();
            return CompletableFuture.supplyAsync(() -> {
                checkVersion(originalVersion);
                return callback.apply(coord);
            }, executor);
        }

        @Override
        public CompletableFuture<?> scheduleOnce(Consumer<TopologyCoordinator> callback, Duration delay) {
            final int originalVersion = versionSupplier.getAsInt();
            ListenableScheduledFuture<?> listeningFut = executor.schedule(
                    () -> {
                checkVersion(originalVersion);
                callback.accept(coord);
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
            return toCompletableFuture(listeningFut);
        }

        @Override
        public <E> CompletableFuture<?> andThenConsumeAsync(CompletionStage<E> stage, BiConsumer<TopologyCoordinator, E> consumer) {
            final int originalVersion = versionSupplier.getAsInt();
            return stage.thenAcceptAsync(e -> {
                checkVersion(originalVersion);
                consumer.accept(coord, e);
            }, executor)
                    .toCompletableFuture();
        }
    }

}
