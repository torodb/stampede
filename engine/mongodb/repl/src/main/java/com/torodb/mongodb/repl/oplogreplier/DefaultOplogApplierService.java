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

import com.eightkdata.mongowp.OpTime;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.ReadOplogTransaction;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.oplogreplier.OplogApplier.ApplyingJob;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher.ContinuousOplogFetcherFactory;
import com.torodb.mongodb.repl.oplogreplier.fetcher.OplogFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

/**
 * A {@link OplogApplierService} that delegate on an {@link OplogApplier}.
 *
 * A new {@link ContinuousOplogFetcher} and a new {@link ApplyingJob} are created when this service
 * start up and they are finished once the service stop.
 */
public class DefaultOplogApplierService extends IdleTorodbService implements OplogApplierService {

  private static final Logger LOGGER = LogManager.getLogger(DefaultOplogApplierService.class);
  private final OplogApplier oplogApplier;
  private final ContinuousOplogFetcherFactory oplogFetcherFactory;
  private final OplogManager oplogManager;
  private final Callback callback;
  private volatile boolean stopping;
  private OplogFetcher fetcher;
  private ApplyingJob applyJob;
  private final ReplicationFilters replFilters;
  private final ExecutorService selfExecutor;
  private CompletableFuture<Void> onFinishFuture;

  @Inject
  public DefaultOplogApplierService(ThreadFactory threadFactory,
      OplogApplier oplogApplier, OplogManager oplogManager,
      ContinuousOplogFetcherFactory oplogFetcherFactory,
      @Assisted Callback callback, ReplicationFilters replFilters,
      ConcurrentToolsFactory concurrentToolsFactory) {
    super(threadFactory);
    this.oplogApplier = oplogApplier;
    this.oplogFetcherFactory = oplogFetcherFactory;
    this.oplogManager = oplogManager;
    this.callback = callback;
    this.replFilters = replFilters;
    this.selfExecutor = concurrentToolsFactory.createExecutorService("oplog-applier-service", true,
        1);
  }

  @Override
  protected void startUp() throws Exception {
    callback.waitUntilStartPermision();
    fetcher = createFetcher();
    applyJob = oplogApplier.apply(fetcher, new ApplierContext.Builder()
        .setReapplying(false)
        .setUpdatesAsUpserts(true)
        .build()
    );

    onFinishFuture = applyJob.onFinish().thenAcceptAsync(tuple -> {
      if (stopping) {
        return;
      }
      switch (tuple.v1) {
        case FINE:
        case ROLLBACK:
          callback.rollback(this, (RollbackReplicationException) tuple.v2);
          break;
        case UNEXPECTED:
        case STOP:
          callback.onError(this, tuple.v2);
          break;
        case CANCELLED:
          callback.onError(
              this,
              new AssertionError("Unexpected cancellation of the applier while the "
                  + "service is not stopping"));
          break;
        default:
          callback.onError(
              this,
              new AssertionError("Unexpected "
                  + OplogApplier.ApplyingJobFinishState.class.getSimpleName()
                  + " found: " + tuple.v1 + " with error "
                  + tuple.v2));
      }
    }, selfExecutor);
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.debug("Shutdown requested");
    stopping = true;
    if (applyJob != null) {
      if (!applyJob.onFinish().isDone()) {
        LOGGER.trace("Applier has been already finished");
      } else {
        LOGGER.trace("Requesting to stop the stream");
        applyJob.cancel();
        applyJob.onFinish().join();
        LOGGER.trace("Applier finished");
      }
    } else {
      LOGGER.debug(serviceName() + " stopped before it was running?");
    }
    if (fetcher != null) {
      LOGGER.trace("Closing the fetcher");
      fetcher.close();
      LOGGER.trace("Fetcher closed");
    } else {
      LOGGER.debug(serviceName() + " stopped before it was running?");
    }
    if (onFinishFuture != null) {
      onFinishFuture.join();
    }
    List<Runnable> pendingTasks = selfExecutor.shutdownNow();
    assert pendingTasks.isEmpty() : "Oplog applier service shutted down "
        + "before its task were correctly executed";

    callback.onFinish(this);
  }

  private OplogFetcher createFetcher() {
    OpTime lastAppliedOptime;
    long lastAppliedHash;
    try (ReadOplogTransaction oplogReadTrans = oplogManager.createReadTransaction()) {
      lastAppliedOptime = oplogReadTrans.getLastAppliedOptime();
      lastAppliedHash = oplogReadTrans.getLastAppliedHash();
    }

    return replFilters.filterOplogFetcher(
        oplogFetcherFactory.createFetcher(lastAppliedHash, lastAppliedOptime)
    );
  }
}
