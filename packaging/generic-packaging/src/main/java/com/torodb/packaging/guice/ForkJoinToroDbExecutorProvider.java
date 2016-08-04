
package com.torodb.packaging.guice;

import com.torodb.concurrent.ForkJoinToroDbExecutor;
import com.torodb.concurrent.ToroDbExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class ForkJoinToroDbExecutorProvider implements Provider<ToroDbExecutorService>{

    private final int parallelism;
    private final ForkJoinWorkerThreadFactory threadFactory;

    @Inject
    public ForkJoinToroDbExecutorProvider(int parallelism, ForkJoinWorkerThreadFactory threadFactory) {
        this.parallelism = parallelism;
        this.threadFactory = threadFactory;
    }

    @Override
    public ToroDbExecutorService get() {
//        ForkJoinPool actualExecutor = new ForkJoinPool(
//                parallelism,
//                threadFactory,
//                null,
//                true
//        );
        ForkJoinPool actualExecutor = new ForkJoinPool(parallelism,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, true);
        return new ForkJoinToroDbExecutor(actualExecutor);
    }
}
