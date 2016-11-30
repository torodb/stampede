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

import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.ApplyingJob;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.ApplyingJobFinishState;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.UnexpectedOplogApplierException;
import org.jooq.lambda.tuple.Tuple2;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 *
 */
public abstract class AbstractApplyingJob implements ApplyingJob {

  private final CompletableFuture<Tuple2<ApplyingJobFinishState, Throwable>> onFinish;

  public AbstractApplyingJob(CompletableFuture<Empty> onFinish) {
    this.onFinish = onFinish.handle(this::toTuple);
  }

  protected Tuple2<ApplyingJobFinishState, Throwable> toTuple(Empty empty, Throwable t) {
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
        return new Tuple2<>(ApplyingJobFinishState.UNEXPECTED, new UnexpectedOplogApplierException(
            cause));
      }
    }
  }

  @Override
  public CompletableFuture<Tuple2<ApplyingJobFinishState, Throwable>> onFinish() {
    return onFinish;
  }

  @Override
  public void waitUntilFinished() throws StopReplicationException,
      RollbackReplicationException, CancellationException,
      UnexpectedOplogApplierException {
    Tuple2<ApplyingJobFinishState, Throwable> tuple = onFinish().join();
    switch (tuple.v1) {
      case FINE:
        return;
      case CANCELLED:
        throw (CancellationException) tuple.v2;
      case ROLLBACK:
        throw (RollbackReplicationException) tuple.v2;
      case STOP:
        throw (StopReplicationException) tuple.v2;
      case UNEXPECTED:
        throw (UnexpectedOplogApplierException) tuple.v2;
      default:
        throw new AssertionError("Unexpected tuple finish state "
            + tuple.v1);
    }
  }
}
