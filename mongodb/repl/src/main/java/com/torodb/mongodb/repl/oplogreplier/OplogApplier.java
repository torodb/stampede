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

package com.torodb.mongodb.repl.oplogreplier;

import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.jooq.lambda.tuple.Tuple2;

/**
 *
 */
public interface OplogApplier extends AutoCloseable {

    /**
     * Applies the {@link OplogBatch} fetched by the given {@link OplogFetcher}, modifying the
     * {@link OplogManager}.
     *
     *
     * @param fetcher
     * @param context
     * @return A {@link CompletableFuture} that will be done once the replication finishes
     */
    public ApplyingJob apply(OplogFetcher fetcher, ApplierContext context);

    public static interface ApplyingJob {

        /**
         * Returns a future that will be done once the applying process finishes.
         *
         * It is not recommended to delegate on this method. Try to use type safe methods
         * {@link #onFinish() } or {@link #waitUntilFinished() } instead.
         *
         * @return a future that will be done once the applying process finishes
         */
        CompletableFuture<Empty> onFinishRaw();

        /**
         * Returns a future that will be done once the applying process finishes.
         *
         * This future should never finish exceptionally, as all exceptions are captured and
         * returned as a tuple with the {@link ApplyingJobFinishState finish state} and the
         * exception thrown.
         *
         * This future will complete after {@link #onFinishRaw() } is called.
         * @return
         */
        default CompletableFuture<Tuple2<ApplyingJobFinishState, Throwable>> onFinish() {
            return onFinishRaw().handle((e, t) -> {
                if (t == null) {
                    return new Tuple2<>(ApplyingJobFinishState.FINE, null);
                } else {
                    Throwable cause;
                    if (t instanceof CompletionException) {
                        cause = t.getCause();
                    } else {
                        cause = t;
                    }
                    if (cause instanceof RollbackReplicationException) {
                        return new Tuple2<>(ApplyingJobFinishState.ROLLBACK, cause);
                    } else if (cause instanceof StopReplicationException) {
                        return new Tuple2<>(ApplyingJobFinishState.STOP, cause);
                    } else if (cause instanceof CancellationException) {
                        return new Tuple2<>(ApplyingJobFinishState.CANCELLED, cause);
                    } else {
                        return new Tuple2<>(ApplyingJobFinishState.UNEXPECTED, new UnexpectedOplogApplierException(cause));
                    }
                }
            });
        }

        /**
         * Waits until the applying process finishes, throwing an exception if it fails.
         *
         * @throws StopReplicationException
         * @throws RollbackReplicationException
         * @throws CancellationException if the execution was cancelled.
         * @throws UnexpectedOplogApplierException if any other throwable was thrown
         * @see #cancel() 
         */
        default void waitUntilFinished() throws StopReplicationException, RollbackReplicationException, CancellationException, UnexpectedOplogApplierException {
            Tuple2<ApplyingJobFinishState, Throwable> tuple = onFinish().join();
            switch (tuple.v1) {
                case FINE: return ;
                case CANCELLED: throw (CancellationException) tuple.v2;
                case ROLLBACK: throw (RollbackReplicationException) tuple.v2;
                case STOP: throw (StopReplicationException) tuple.v2;
                case UNEXPECTED: throw (UnexpectedOplogApplierException) tuple.v2;
                default:
                    throw new AssertionError("Unexpected tuple finish state " + tuple.v1);
            }
        }

        /**
         * Cancels the applying process and return a future that will be done once the cancellation
         * is done.
         *
         * Before this future completes, {@link #onFinishRaw() } will be called, but
         * {@link #onFinish() } can be called before, after or at the same time this future
         * completes.
         *
         * @return
         */
        CompletableFuture<Empty> cancel();
    }

    public static enum ApplyingJobFinishState {
        FINE, ROLLBACK, STOP, CANCELLED, UNEXPECTED;
    }

    public static class UnexpectedOplogApplierException extends Exception {

        private static final long serialVersionUID = 5088795687088789661L;

        private UnexpectedOplogApplierException(Throwable cause) {
            super(cause);
        }

    }
}
