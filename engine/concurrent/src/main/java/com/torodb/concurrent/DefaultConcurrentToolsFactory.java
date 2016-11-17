/*
 * ToroDB - ToroDB: Concurrent Utils
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.concurrent;

import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.StreamExecutor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.*;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.function.Function;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import javax.inject.Inject;

/**
 *
 */
public class DefaultConcurrentToolsFactory implements ConcurrentToolsFactory {
    
    private final BlockerThreadFactoryFunction blockerThreadFactoryFunction;
    private final ForkJoinThreadFactoryFunction forkJoinThreadFactoryFunction;
    private final int defaultThreads;
    private final ExecutorServiceShutdownHelper shutdownHelper;

    @Inject
    public DefaultConcurrentToolsFactory(BlockerThreadFactoryFunction blockerThreadFactoryFunction,
            ForkJoinThreadFactoryFunction forkJoinThreadFactoryFunction,
            @ParallelLevel int parallelLevel, ExecutorServiceShutdownHelper shutdownHelper) {
        this.blockerThreadFactoryFunction = blockerThreadFactoryFunction;
        this.forkJoinThreadFactoryFunction = forkJoinThreadFactoryFunction;
        this.defaultThreads = parallelLevel;
        this.shutdownHelper = shutdownHelper;
    }

    @Override
    public int getDefaultMaxThreads() {
        return defaultThreads;
    }

    @Override
    public StreamExecutor createStreamExecutor(String prefix,
            boolean blockerTasks, int maxThreads) {
        return new AkkaStreamExecutor(
                blockerThreadFactoryFunction.apply(prefix),
                maxThreads,
                createExecutorService(prefix, blockerTasks, maxThreads),
                prefix
        );
    }

    @Override
    public ScheduledExecutorService createScheduledExecutorServiceWithMaxThreads(
            String prefix, int maxThreads) {
        ThreadFactory threadFactory = blockerThreadFactoryFunction.apply(prefix);
        ScheduledThreadPoolExecutor executorService =
                new ScheduledThreadPoolExecutor(maxThreads, threadFactory);
        shutdownHelper.terminateOnShutdown(prefix, executorService);

        return executorService;
    }

    @Override
    public ExecutorService createExecutorServiceWithMaxThreads(
            String prefix, int maxThreads) {
        ThreadFactory threadFactory = blockerThreadFactoryFunction.apply(prefix);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                maxThreads, maxThreads,
                10L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                threadFactory);
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        shutdownHelper.terminateOnShutdown(prefix, threadPoolExecutor);
        return threadPoolExecutor;
    }

    @Override
    @SuppressFBWarnings(value = {"NP_NONNULL_PARAM_VIOLATION"},
            justification = "ForkJoinPool constructor admits a null "
                    + "UncaughtExceptionHandler")
    public ExecutorService createExecutorService(String prefix,
            boolean blockerTasks, int maxThreads) {
        ExecutorService executorService;
        if (blockerTasks) {
            ThreadFactory threadFactory
                    = blockerThreadFactoryFunction.apply(prefix);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                    maxThreads, maxThreads,
                    10L, TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>(),
                    threadFactory);
            threadPoolExecutor.allowCoreThreadTimeOut(true);
            executorService = threadPoolExecutor;
        } else {
            ForkJoinWorkerThreadFactory threadFactory
                    = forkJoinThreadFactoryFunction.apply(prefix);
            executorService = new ForkJoinPool(maxThreads, threadFactory,
                    null, true);
        }
        shutdownHelper.terminateOnShutdown(prefix, executorService);
        return executorService;
    }

    public static interface BlockerThreadFactoryFunction extends
            Function<String, ThreadFactory> {

    }

    public static interface ForkJoinThreadFactoryFunction extends
            Function<String, ForkJoinWorkerThreadFactory> {

    }

}
