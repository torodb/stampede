
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
