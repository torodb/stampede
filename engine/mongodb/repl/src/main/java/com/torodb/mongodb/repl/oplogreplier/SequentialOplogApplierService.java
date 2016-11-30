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
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.assistedinject.Assisted;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.OplogManager;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.oplogreplier.fetcher.ContinuousOplogFetcher.ContinuousOplogFetcherFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * The old {@link OplogApplierService} which uses a very simmilar algorithm to the one that MongoDB
 * uses.
 */
@ThreadSafe
public class SequentialOplogApplierService extends IdleTorodbService
    implements OplogApplierService {

  /**
   * The maximum capacity of the {@linkplain #fetchQueue}.
   */
  private static final int BUFFER_CAPACITY = 1024;
  private static final Logger LOGGER = LogManager.getLogger(SequentialOplogApplierService.class);

  private final ReentrantLock mutex = new ReentrantLock();
  /**
   * A queue used to store fetched oplogs to be applied on this node.
   */
  private final MyQueue fetchQueue;

  private final Callback callback;
  private final OplogManager oplogManager;
  private final OplogOperationApplier oplogOpApplier;
  private final MongodServer server;
  private final Condition allApplied;
  private final ThreadFactory threadFactory;
  private final Executor executor;
  private final ContinuousOplogFetcherFactory oplogFetcherFactory;

  private boolean paused;
  private boolean pauseRequested;
  private boolean fetcherIsPaused;
  private final Condition fetcherPausedCond;
  private final Condition fetcherCanContinueCond;

  private ReplSyncFetcher fetcherService;
  private ReplSyncApplier applierService;

  @Inject
  SequentialOplogApplierService(
      @TorodbIdleService ThreadFactory threadFactory,
      @Assisted Callback callback,
      OplogManager oplogManager,
      OplogOperationApplier oplogOpApplier,
      MongodServer server,
      ContinuousOplogFetcherFactory oplogFetcherFactory) {
    super(threadFactory);
    this.callback = callback;
    this.fetchQueue = new MyQueue();
    this.oplogManager = oplogManager;
    this.oplogOpApplier = oplogOpApplier;
    this.server = server;
    this.allApplied = mutex.newCondition();
    this.fetcherPausedCond = mutex.newCondition();
    this.fetcherCanContinueCond = mutex.newCondition();
    this.threadFactory = threadFactory;
    final ThreadFactory utilityThreadFactory = new ThreadFactoryBuilder()
        .setThreadFactory(threadFactory)
        .setNameFormat("repl-secondary-util-%d")
        .build();
    this.executor = (Runnable command) -> {
      utilityThreadFactory.newThread(command).start();
    };
    this.oplogFetcherFactory = oplogFetcherFactory;
  }

  @Override
  protected void startUp() {
    callback.waitUntilStartPermision();
    LOGGER.info("Starting SECONDARY service");
    paused = false;
    fetcherIsPaused = false;
    pauseRequested = false;

    long lastAppliedHash;
    OpTime lastAppliedOptime;
    try (OplogManager.ReadOplogTransaction oplogReadTrans = oplogManager.createReadTransaction()) {
      lastAppliedHash = oplogReadTrans.getLastAppliedHash();
      lastAppliedOptime = oplogReadTrans.getLastAppliedOptime();
    }

    fetcherService = new ReplSyncFetcher(
        threadFactory,
        new FetcherView(),
        oplogFetcherFactory.createFetcher(lastAppliedHash, lastAppliedOptime)
    );
    fetcherService.startAsync();
    applierService = new ReplSyncApplier(
        threadFactory,
        oplogOpApplier,
        server,
        oplogManager,
        new ApplierView()
    );
    applierService.startAsync();

    fetcherService.awaitRunning();
    applierService.awaitRunning();

    LOGGER.info("Started SECONDARY service");
  }

  @Override
  protected void shutDown() {
    fetcherService.stopAsync();
    applierService.stopAsync();

    fetcherService.awaitTerminated();
    applierService.awaitTerminated();
  }

  public boolean isPaused() {
    return paused;
  }

  private final class FetcherView implements ReplSyncFetcher.SyncServiceView {

    @Override
    public void deliver(OplogOperation oplogOp) throws InterruptedException {
      fetchQueue.addLast(oplogOp);
    }

    @Override
    public void rollback(RollbackReplicationException ex) {
      executor.execute(() -> {
        callback.rollback(SequentialOplogApplierService.this, ex);
      });
    }

    @Override
    @SuppressFBWarnings(value = {"WA_AWAIT_NOT_IN_LOOP"},
        justification =
        "This class seem deprecated. We just ignore the warning even if it is correct")
    public void awaitUntilUnpaused() throws InterruptedException {
      mutex.lock();
      try {
        fetcherIsPaused = true;
        fetcherPausedCond.signalAll();
        fetcherCanContinueCond.await();
      } finally {
        mutex.unlock();
      }
    }

    @Override
    public boolean shouldPause() {
      return pauseRequested;
    }

    @Override
    public void awaitUntilAllFetchedAreApplied() {
      mutex.lock();
      try {
        while (!fetchQueue.isEmpty()) {
          allApplied.awaitUninterruptibly();
        }
      } finally {
        mutex.unlock();
      }
    }

    @Override
    public void fetchFinished() {
    }

    @Override
    public void fetchAborted(final Throwable ex) {

      executor.execute(() -> {
        callback.onError(SequentialOplogApplierService.this, ex);
      });
    }

  }

  private final class ApplierView implements ReplSyncApplier.SyncServiceView {

    @Override
    public List<OplogOperation> takeOps() throws InterruptedException {
      //TODO: Improve this class to be able to return more than one action per call!
      //To do that, some changes must be done to avoid concurrency problems while
      //the fetcher service is working
      OplogOperation first = fetchQueue.getFirst();
      return Collections.singletonList(first);
    }

    @Override
    public void markAsApplied(OplogOperation oplogOperation) {
      fetchQueue.removeLast(oplogOperation);
    }

    @Override
    public boolean failedToApply(OplogOperation oplogOperation, Status<?> status) {
      executor.execute(() -> {
        LOGGER.error("Secondary state failed to apply an operation: {}", status);
        callback.onError(SequentialOplogApplierService.this, new MongoException(status));
      });
      return false;
    }

    @Override
    public boolean failedToApply(OplogOperation oplogOperation, final Throwable t) {
      executor.execute(() -> {
        LOGGER.error("Secondary state failed to apply an operation", t);
        callback.onError(SequentialOplogApplierService.this, t);
      });
      return false;
    }

    @Override
    public boolean failedToApply(OplogOperation oplogOperation,
        final OplogManagerPersistException t) {
      executor.execute(() -> {
        LOGGER.error("Secondary state failed to apply an operation", t);
        callback.onError(SequentialOplogApplierService.this, t);
      });
      return false;
    }

    @Override
    public SupervisorDecision onError(Object supervised, Throwable t) {
      executor.execute(() -> {
        LOGGER.error("Secondary state failed", t);
        callback.onError(SequentialOplogApplierService.this, t);
      });
      return SupervisorDecision.STOP;
    }
  }

  /**
   * A simplification of a {@link ArrayBlockingQueue} that use the same lock as the container class.
   */
  private class MyQueue {

    private final OplogOperation[] buffer = new OplogOperation[BUFFER_CAPACITY];
    private final Condition notEmpty = mutex.newCondition();
    private final Condition notFull = mutex.newCondition();
    private int iFirst = 0;
    private int iLast = 0;
    private int count = 0;

    /**
     * Circularly increment i.
     */
    final int inc(int i) {
      return (++i == BUFFER_CAPACITY) ? 0 : i;
    }

    /**
     * Circularly decrement i.
     */
    final int dec(int i) {
      return ((i == 0) ? BUFFER_CAPACITY : i) - 1;
    }

    private boolean isEmpty() {
      return count == 0;
    }

    private void addLast(OplogOperation op) throws InterruptedException {
      if (op == null) {
        throw new NullPointerException();
      }
      final OplogOperation[] items = this.buffer;
      final ReentrantLock mutex = SequentialOplogApplierService.this.mutex;
      mutex.lockInterruptibly();
      try {
        try {
          while (count == BUFFER_CAPACITY) {
            notFull.await();
          }
        } catch (InterruptedException ex) {
          notFull.signal();
          throw ex;
        }
        items[iLast] = op;
        iLast = inc(iLast);
        ++count;
        notEmpty.signal();
      } finally {
        mutex.unlock();
      }
    }

    private OplogOperation getFirst() throws InterruptedException {
      final OplogOperation[] items = this.buffer;
      final ReentrantLock mutex = SequentialOplogApplierService.this.mutex;
      mutex.lock();
      try {
        while (isEmpty()) {
          notEmpty.await();
        }
        return items[iFirst];
      } finally {
        mutex.unlock();
      }
    }

    private void removeLast(OplogOperation sign) {
      final OplogOperation[] items = this.buffer;
      final ReentrantLock mutex = SequentialOplogApplierService.this.mutex;
      mutex.lock();
      try {
        if (count == 0) {
          throw new IllegalStateException("The queue is empty");
        }
        OplogOperation first = items[iFirst];
        if (first != sign) {
          throw new IllegalArgumentException("There given operation "
              + "sign is not the same as the first element to "
              + "read");
        }
        items[iFirst] = null;
        iFirst = inc(iFirst);
        --count;
        notFull.signal();
      } finally {
        mutex.unlock();
      }
    }
  }
}
