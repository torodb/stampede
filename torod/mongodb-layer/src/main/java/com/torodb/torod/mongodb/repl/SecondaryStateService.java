
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.util.concurrent.AbstractIdleService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class control the sync process.
 *
 * This process is used when the node is storing data and it is following
 * another. For example: synchronization is used when the node is in
 * {@linkplain MemberState#RS_SECONDARY secondary} or
 * {@linkplain MemberState#RS_RECOVERING recovering} states but not when it is
 * {@linkplain MemberState#RS_PRIMARY primary}.
 */
@ThreadSafe
class SecondaryStateService extends AbstractIdleService {

    /**
     * The maximum capacity of the {@linkplain #fetchQueue}.
     */
    private static final int BUFFER_CAPACITY = 1024;
    private static final Logger LOGGER
            = LoggerFactory.getLogger(SecondaryStateService.class);

    private final ReentrantLock mutex = new ReentrantLock();
    /**
     * A queue used to store fetched oplogs to be applied on this node.
     */
    private final MyQueue fetchQueue;

    private final Callback callback;
    private final OplogManager oplogManager;
    private final Executor executor;
    private final Condition allApplied;
    private final OplogReader reader;

    private boolean paused;
    private boolean pauseRequested;
    private boolean fetcherIsPaused;
    private final Condition fetcherPausedCond;
    private final Condition fetcherCanContinueCond;

    private ReplSyncFetcher fetcherService;
    private ReplSyncApplier applierService;

    SecondaryStateService(
            Callback callback,
            OplogManager oplogManager,
            OplogReader reader,
            Executor executor) {
        this.callback = callback;
        this.fetchQueue = new MyQueue();
        this.reader = reader;
        this.oplogManager = oplogManager;
        this.executor = executor;
        this.allApplied = mutex.newCondition();
        this.fetcherPausedCond = mutex.newCondition();
        this.fetcherCanContinueCond = mutex.newCondition();
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() {
        paused = false;
        fetcherIsPaused = false;
        pauseRequested = false;

        OplogManager.ReadTransaction oplogReadTrans = oplogManager.createReadTransaction();
        try {
            long lastAppliedHash = oplogReadTrans.getLastAppliedHash();
            OpTime lastAppliedOptime = oplogReadTrans.getLastAppliedOptime();

            fetcherService = new ReplSyncFetcher(
                    executor,
                    new FetcherView(),
                    reader,
                    lastAppliedHash,
                    lastAppliedOptime
            );
            fetcherService.startAsync();
            applierService = new ReplSyncApplier(executor, new ApplierView());
            applierService.startAsync();
        } finally {
            oplogReadTrans.close();
        }
    }

    @Override
    protected void shutDown() {
        fetcherService.stopAsync();
        applierService.stopAsync();

        fetcherService.awaitTerminated();
        applierService.awaitTerminated();

        reader.close();
    }

    public boolean isPaused() {
        return paused;
    }

    /**
     * Pauses the sync process.
     *
     * The calling thread will await until all fetched operations are applied.
     */
    public void doPause() {
        mutex.lock();
        try {
            if (isPaused()) {
                return ;
            }
            pauseRequested = true;
            while (!fetcherIsPaused) {
                fetcherPausedCond.awaitUninterruptibly();
            }

            while(!fetchQueue.isEmpty()) {
                allApplied.awaitUninterruptibly();
            }
            paused = true;
        } finally {
            mutex.unlock();
        }
    }

    public void doContinue() {
        mutex.lock();
        try {
            if (!isPaused()) {
                return ;
            }
            pauseRequested = false;
            fetcherCanContinueCond.signalAll();

            paused = false;
        } finally {
            mutex.unlock();
        }
    }
    
    interface Callback {

        void rollbackRequired();
   }

    private final class FetcherView implements ReplSyncFetcher.SyncServiceView {

        @Override
        public void deliver(OplogOperation oplogOp) throws InterruptedException {
            fetchQueue.addLast(oplogOp);
        }

        @Override
        public void rollback(OplogReader reader) {
            executor.execute(
                    new Runnable() {

                        @Override
                        public void run() {
                            callback.rollbackRequired();
                        }
                    }
            );
        }

        @Override
        public void awaitUntilUnpaused() throws InterruptedException {
            mutex.lock();
            try {
                while (pauseRequested) {
                    fetcherIsPaused = true;
                    fetcherPausedCond.signalAll();
                    fetcherCanContinueCond.await();
                }
            } finally {
                mutex.unlock();
            }
        }

        @Override
        public boolean shouldPause() {
            return pauseRequested;
        }

    }

    private final class ApplierView implements ReplSyncApplier.SyncServiceView {

        @Override
        public OplogOperation takeOp() throws InterruptedException {
            return fetchQueue.getFirst();
        }

        @Override
        public void markAsApplied(OplogOperation oplogOperation) {
            fetchQueue.removeLast(oplogOperation);
        }
    }

    /**
     * A simplification of a {@link ArrayBlockingQueue} that use the same lock
     * as the container class.
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
            return count != 0;
        }

        private void addLast(OplogOperation op) throws InterruptedException {
            if (op == null) {
                throw new NullPointerException();
            }
            final OplogOperation[] items = this.buffer;
            final ReentrantLock mutex = SecondaryStateService.this.mutex;
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
            final ReentrantLock mutex = SecondaryStateService.this.mutex;
            mutex.lock();
            try {
                while (!isEmpty()) {
                    notEmpty.await();
                }
                return items[iFirst];
            } finally {
                mutex.unlock();
            }
        }

        private void removeLast(OplogOperation sign) {
            final OplogOperation[] items = this.buffer;
            final ReentrantLock mutex = SecondaryStateService.this.mutex;
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
