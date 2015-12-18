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

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that blocks new tasks when heap memory pressure is high and
 * delegates the session executors on another {@linkplain TPSExecutorFactory
 * factory}.
 */
public class MemBlockingExecutorServiceProvider extends AbstractExecutorServiceProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemBlockingExecutorServiceProvider.class
    );
    private static final double MAX_RECOMMENDED_GRANTED_MEMORY = 0.8;
    private static final double MIN_RECOMMENDED_GRANTED_MEMORY = 0.15;
    private final Runtime runtime;
    private final double maxUsedMemPercentage;
    private final AbstractExecutorServiceProvider delegate;
    private final ExecutorService waitExecutor;
    private final long maxWaitingTime;
    private final SubmitPermissionCallback submitPermissionCallback;
    private final Runnable waitSubmitTask;

    public MemBlockingExecutorServiceProvider(
            double grantedMemPercentage,
            long maxWaitTime,
            AbstractExecutorServiceProvider delegate) {
        Preconditions.checkArgument(
                grantedMemPercentage < 1,
                "Granted memory must be lower than 1"
        );
        Preconditions.checkArgument(
                grantedMemPercentage > 0,
                "Granted memory must be higher than 0"
        );
        if (grantedMemPercentage > MAX_RECOMMENDED_GRANTED_MEMORY) {
            LOGGER.warn("Granted memory requested (=" + grantedMemPercentage
                    + ") is higher than the maximum recommended value "
                    + "(=" + MAX_RECOMMENDED_GRANTED_MEMORY + ")");
        }
        if (grantedMemPercentage < MIN_RECOMMENDED_GRANTED_MEMORY) {
            LOGGER.warn("Granted memory requested (=" + grantedMemPercentage
                    + ") is lower than the minimum recommended value "
                    + "(=" + MAX_RECOMMENDED_GRANTED_MEMORY + ")");
        }
        this.maxUsedMemPercentage = 1 - grantedMemPercentage;
        this.runtime = Runtime.getRuntime();
        this.delegate = delegate;
        this.waitExecutor = Executors.newSingleThreadExecutor(
                new MyThreadFactory("wait-submit", Thread.NORM_PRIORITY - 1)
        );
        this.maxWaitingTime = maxWaitTime;
        this.submitPermissionCallback = new WaitForSubmitPermissionCallback();
        this.waitSubmitTask = new WaitSubmitTask();
    }

    @Override
    protected ExecutorService createExecutorService(String name, int priority) {
        return new MyThreadPoolExecutor(
                delegate.createExecutorService(name, priority),
                submitPermissionCallback
        );
    }

    private class WaitForSubmitPermissionCallback implements
            SubmitPermissionCallback {

        @Override
        public void waitForPermission(Runnable command) throws
                RejectedExecutionException {
            Future<?> waitTask = waitExecutor.submit(
                    waitSubmitTask
            );
            try {
                waitTask.get();
            }
            catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            catch (ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }

    }
    
    private class WaitSubmitTask implements Runnable {

        @SuppressFBWarnings("UW_UNCOND_WAIT")
        @Override
        public void run() {
            long waitTime = 1;
                boolean isMemSaturated = isMemSaturated();
                long startNanos = System.nanoTime();
                boolean hadToStop = isMemSaturated;
                while (isMemSaturated) {
                    try {
                        synchronized (this) {
                            LOGGER.debug(
                                    "Mem is saturated -> waiting {} ms",
                                    waitTime
                            );
                            this.wait(waitTime);
                            isMemSaturated = isMemSaturated();
                            if (waitTime >= maxWaitingTime && isMemSaturated) {
                                resolveLongWait();
                            }
                            waitTime = Math.min(waitTime << 2, maxWaitingTime);
                        }
                    }
                    catch (InterruptedException ex) {
                        //TODO: Check exception
                        throw new RuntimeException("Interrupted thread");
                    }
                }
                if (hadToStop) {
                    long waitedMicros = (System.nanoTime() - startNanos)
                            / 1000000;
                    LOGGER.debug("Task had to wait {} ms", waitedMicros);
                }
        }
        
    }

    @SuppressFBWarnings("DM_GC")
    private void resolveLongWait() {
        LOGGER.warn("Getting out of memory. Explicity calling GC");
        /*
         * Yes, here we call gc() manually, despite having no guarantees that it
         * will be actually called. By performing a runFinalization()
         * afterwards, unused memory should be freed
         * (http://stackoverflow.com/a/23774873).
         *
         * This code path will only be reached as a last resort under high
         * memory pressure in ToroDB. ToroDB is a highly concurrent,
         * asynchronous server. ToroDB performs control flow per client, but
         * there is no reason many concurrent clients may overload the server.
         * Under high memory pressure, the JVM may perform gc too late and
         * OutOfMemoryError may be thrown. We try to avoid this situation here
         * by calling gc() explicitly
         *
         */
        System.gc();
        System.runFinalization();
    }

    private boolean isMemSaturated() {
        long maxMemory = runtime.maxMemory();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();

        double highLevel = maxMemory * maxUsedMemPercentage;
        return usedMemory >= highLevel;
    }

    private static class MyThreadPoolExecutor extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final SubmitPermissionCallback submitPermissionFunction;

        public MyThreadPoolExecutor(
                ExecutorService delegate,
                SubmitPermissionCallback submitPermissionFunction) {
            this.delegate = delegate;
            this.submitPermissionFunction = submitPermissionFunction;
        }

        @Override
        public void execute(Runnable command) {
            submitPermissionFunction.waitForPermission(command);
            delegate.execute(command);
        }

        @Override
        public synchronized void shutdown() {
            delegate.shutdown();
        }

        @Override
        public synchronized List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws
                InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

    }

    private static interface SubmitPermissionCallback {

        public void waitForPermission(Runnable command) throws
                RejectedExecutionException;
    }
    
    private static class MyThreadFactory implements ThreadFactory {
        private final String name;
        private final int priority;

        private MyThreadFactory(
                String name, 
                int priority) {
            this.name = name;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable r) {
            
            Thread t = new Thread(
                    r, 
                    name
            );
            t.setPriority(priority);
            return t;
        }
    }
}
