/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.backends.executor.servicefactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

/**
 *
 */
public class LazyBlockingExecutorServiceProvider extends AbstractExecutorServiceProvider {

    private final AtomicInteger threadNumberCounter;
    private final ThreadGroup threadGroup;
    private final int queueSize;

    public LazyBlockingExecutorServiceProvider(int queueSize) {
        this.queueSize = queueSize;
        this.threadNumberCounter = new AtomicInteger(0);
        this.threadGroup = new ThreadGroup("torodb-executor");
    }

    @Override
    protected ExecutorService createExecutorService(String name, int priority) {
        return new BlockingThreadPoolExecutor(
                queueSize,
                new MyThreadFactory(
                        name + "-" + threadNumberCounter.incrementAndGet(), 
                        threadGroup, 
                        priority
                )
        );
    }
    
    private static class MyThreadFactory implements ThreadFactory {
        private final String name;
        private final ThreadGroup threadGroup;
        private final int priority;

        private MyThreadFactory(
                String name, 
                ThreadGroup threadGroup, 
                int priority) {
            this.name = name;
            this.threadGroup = threadGroup;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            
            Thread t = new Thread(
                    threadGroup, 
                    r, 
                    name
            );
            t.setPriority(priority);
            return t;
        }
    }

    private static class BlockingThreadPoolExecutor extends ThreadPoolExecutor {

        private final ReentrantLock lock;
        @GuardedBy("lock")
        private final Condition insertionsAllowed;
        private final int queueSize;

        public BlockingThreadPoolExecutor(
                int queueSize,
                ThreadFactory threadFactory) {
            super(
                    1,
                    1,
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(queueSize),
                    threadFactory
            );
            this.queueSize = queueSize;
            lock = new ReentrantLock();
            insertionsAllowed = lock.newCondition();
            super.setRejectedExecutionHandler(
                    new BlockingRejectExecutionHandler(
                            lock, 
                            insertionsAllowed,
                            getQueue(),
                            queueSize
                    )
            );
        }

        /**
         * Method invoked upon completion of execution of the given Runnable, by
         * the thread that executed the task.
         */
        @Override
        protected void afterExecute(final Runnable r, final Throwable t) {
            super.afterExecute(r, t);
            if (getQueue().size() < queueSize / 2) {
                lock.lock();
                try {
                    insertionsAllowed.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    private static class BlockingRejectExecutionHandler implements
            RejectedExecutionHandler {

        private final ReentrantLock lock;
        @GuardedBy("lock")
        private final Condition insertionsAllowed;
        private final BlockingQueue<?> queue;
        private final int queueSize;

        private BlockingRejectExecutionHandler(
                ReentrantLock lock, 
                Condition insertionsAllowed,
                BlockingQueue<?> queue,
                int queueSize) {
            this.lock = lock;
            this.insertionsAllowed = insertionsAllowed;
            this.queue = queue;
            this.queueSize = queueSize;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                lock.lock();
                try {
                    while (queue.size() >= queueSize) {
                        insertionsAllowed.await();
                    }
                    executor.execute(r);
                }
                finally {
                    lock.unlock();
                }
            }
            catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

}
