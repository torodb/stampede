
package com.torodb.concurrent.guice;

import com.torodb.concurrent.ForkJoinToroDbExecutor;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.ToroDbExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class ForkJoinToroDbExecutorProvider implements Provider<ToroDbExecutorService>{

    private static final Logger LOGGER = LogManager.getLogger(ForkJoinToroDbExecutorProvider.class);
    private final int parallelism;
    private final ForkJoinWorkerThreadFactory threadFactory;
    private final ExecutorServiceShutdownHelper shutdownHelper;

    @Inject
    public ForkJoinToroDbExecutorProvider(@ParallelLevel int parallelism,
            ForkJoinWorkerThreadFactory threadFactory, ExecutorServiceShutdownHelper shutdownHelper) {
        this.parallelism = parallelism;
        this.threadFactory = threadFactory;
        this.shutdownHelper = shutdownHelper;
    }

    @Override
    public ToroDbExecutorService get() {
        ForkJoinPool actualExecutor = new ForkJoinPool(
                parallelism,
                threadFactory,
                null,
                true
        );
        ForkJoinToroDbExecutor result = new ForkJoinToroDbExecutor(actualExecutor);

        shutdownHelper.terminateOnShutdown(result);

        return result;
    }
}
