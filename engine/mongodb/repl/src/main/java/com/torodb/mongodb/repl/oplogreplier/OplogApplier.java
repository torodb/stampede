/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.mongodb.repl.oplogreplier;

import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import org.jooq.lambda.tuple.Tuple2;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

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

    default boolean hasFinished() {
      return !onFinish().isDone();
    }

    /**
     * Returns a future that will be done once the applying process finishes.
     * <p>
     * This future should never finish exceptionally, as all exceptions are captured and returned as
     * a tuple with the {@link ApplyingJobFinishState finish state} and the exception thrown.
     *
     * @return
     */
    CompletableFuture<Tuple2<ApplyingJobFinishState, Throwable>> onFinish();

    /**
     * Waits until the applying process finishes, throwing an exception if it fails.
     *
     * @throws StopReplicationException
     * @throws RollbackReplicationException
     * @throws CancellationException           if the execution was cancelled.
     * @throws UnexpectedOplogApplierException if any other throwable was thrown
     * @see #cancel()
     */
    void waitUntilFinished() throws StopReplicationException,
        RollbackReplicationException, CancellationException,
        UnexpectedOplogApplierException;

    /**
     * Cancels the applying process.
     * <p>
     * If the process does not finishes early by other reason, the {@link #onFinish()} future will
     * finished with a tupple whose first element is {@link ApplyingJobFinishState#CANCELLED}.
     */
    void cancel();
  }

  public static enum ApplyingJobFinishState {
    FINE,
    ROLLBACK,
    STOP,
    CANCELLED,
    UNEXPECTED;
  }

  public static class UnexpectedOplogApplierException extends Exception {

    private static final long serialVersionUID = 5088795687088789661L;

    public UnexpectedOplogApplierException(Throwable cause) {
      super(cause);
    }

  }
}
