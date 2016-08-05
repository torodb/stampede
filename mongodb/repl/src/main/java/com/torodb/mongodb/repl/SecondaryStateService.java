
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.assistedinject.Assisted;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class SecondaryStateService extends ThreadFactoryIdleService {

    /**
     * The maximum capacity of the {@linkplain #fetchQueue}.
     */
    private static final int BUFFER_CAPACITY = 1024;
    private static final Logger LOGGER = LogManager.getLogger(SecondaryStateService.class);

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
    private final OplogReaderProvider readerProvider;
    private final SyncSourceProvider syncSourceProvider;
    private final ThreadFactory threadFactory;
    private final Executor executor;

    private boolean paused;
    private boolean pauseRequested;
    private boolean fetcherIsPaused;
    private final Condition fetcherPausedCond;
    private final Condition fetcherCanContinueCond;

    private ReplSyncFetcher fetcherService;
    private ReplSyncApplier applierService;

    @Inject
    SecondaryStateService(
            @ToroDbIdleService ThreadFactory threadFactory,
            @Assisted Callback callback,
            OplogManager oplogManager,
            OplogReaderProvider readerProvider,
            OplogOperationApplier oplogOpApplier,
            MongodServer server,
            SyncSourceProvider syncSourceProvider) {
        super(threadFactory);
        this.callback = callback;
        this.fetchQueue = new MyQueue();
        this.readerProvider = readerProvider;
        this.oplogManager = oplogManager;
        this.oplogOpApplier = oplogOpApplier;
        this.server = server;
        this.allApplied = mutex.newCondition();
        this.fetcherPausedCond = mutex.newCondition();
        this.fetcherCanContinueCond = mutex.newCondition();
        this.syncSourceProvider = syncSourceProvider;
        this.threadFactory = threadFactory;
        final ThreadFactory utilityThreadFactory = new ThreadFactoryBuilder()
                .setThreadFactory(threadFactory)
                .setNameFormat("repl-secondary-util-%d")
                .build();
        this.executor = (Runnable command) -> {
            utilityThreadFactory.newThread(command).start();
        };
    }

    @Override
    protected void startUp() {
        LOGGER.info("Starting SECONDARY service");
        paused = false;
        fetcherIsPaused = false;
        pauseRequested = false;

        try (OplogManager.ReadTransaction oplogReadTrans = oplogManager.createReadTransaction()) {
            long lastAppliedHash = oplogReadTrans.getLastAppliedHash();
            OpTime lastAppliedOptime = oplogReadTrans.getLastAppliedOptime();

            fetcherService = new ReplSyncFetcher(
                    threadFactory,
                    new FetcherView(),
                    syncSourceProvider,
                    readerProvider,
                    lastAppliedHash,
                    lastAppliedOptime
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
        }
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

        void impossibleToRecoverFromError(Status<?> status);

        void impossibleToRecoverFromError(Throwable t);
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

        @Override
        public void awaitUntilAllFetchedAreApplied() {
            mutex.lock();
            try {
                while(!fetchQueue.isEmpty()) {
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

            executor.execute(
                    new Runnable() {

                        @Override
                        public void run() {
                            callback.impossibleToRecoverFromError(ex);
                        }
                    }
            );
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
            executor.execute(
                    new Runnable() {

                        @Override
                        public void run() {
                            LOGGER.error("Secondary state failed to apply an operation: {}", status);
                            callback.impossibleToRecoverFromError(status);
                        }
                    }
            );
            return false;
        }

        @Override
        public boolean failedToApply(OplogOperation oplogOperation, final Throwable t) {
            executor.execute(
                    new Runnable() {

                        @Override
                        public void run() {
                            LOGGER.error("Secondary state failed to apply an operation", t);
                            callback.impossibleToRecoverFromError(t);
                        }
                    }
            );
            return false;
        }

        @Override
        public boolean failedToApply(OplogOperation oplogOperation, final OplogManagerPersistException t) {
            executor.execute(
                    new Runnable() {

                        @Override
                        public void run() {
                            LOGGER.error("Secondary state failed to apply an operation", t);
                            callback.impossibleToRecoverFromError(t);
                        }
                    }
            );
            return false;
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
            return count == 0;
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

    public static interface SecondaryStateServiceFactory {
        SecondaryStateService createSecondaryStateService(Callback callback);
    }
}