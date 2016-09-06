
package com.torodb.concurrent;

import com.torodb.concurrent.guice.ExecutorServiceShutdownHelper;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.StreamExecutor;
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
    public StreamExecutor createStreamExecutor(String prefix, boolean blockerTasks, int maxThreads) {
        return new AkkaStreamExecutor(
                maxThreads,
                createExecutorService(prefix, blockerTasks, maxThreads),
                this::closeExecutor
        );
    }

    @Override
    public StreamExecutor createStreamExecutor(ExecutorService executor, int maxThreads) {
        return new AkkaStreamExecutor(maxThreads, executor, (toClose) -> {});
    }

    private void closeExecutor(ExecutorService executorService) {
        executorService.shutdown();
    }

    @Override
    public ExecutorService createExecutorService(String prefix, boolean blockerTasks, int maxThreads) {
        ExecutorService executorService;
        if (blockerTasks) {
            ThreadFactory threadFactory = blockerThreadFactoryFunction.apply(prefix);
            executorService = Executors.newFixedThreadPool(maxThreads, threadFactory);
        } else {
            ForkJoinWorkerThreadFactory threadFactory = forkJoinThreadFactoryFunction.apply(prefix);
            executorService = new ForkJoinPool(maxThreads, threadFactory, null, true);
        }
        shutdownHelper.terminateOnShutdown(executorService);
        return executorService;
    }

    public static interface BlockerThreadFactoryFunction extends Function<String, ThreadFactory> {

    }

    public static interface ForkJoinThreadFactoryFunction extends Function<String, ForkJoinWorkerThreadFactory> {

    }

}
