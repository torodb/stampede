
package com.torodb.packaging.guice;

import com.torodb.core.ToroDbExecutorService;
import java.util.List;
import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class CachedToroDbExecutorProvider implements Provider<ToroDbExecutorService>{

    private final int maxThreads;
    private final ThreadFactory threadFactory;

    @Inject
    public CachedToroDbExecutorProvider(int maxThreads, ThreadFactory threadFactory) {
        this.maxThreads = maxThreads;
        this.threadFactory = threadFactory;
    }

    @Override
    public ToroDbExecutorService get() {
        ThreadPoolExecutor actualExecutor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        return new FixedToroDbExecutor(actualExecutor);
    }

    private static class FixedToroDbExecutor extends AbstractExecutorService implements ToroDbExecutorService {
        private final ThreadPoolExecutor delegate;

        public FixedToroDbExecutor(ThreadPoolExecutor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
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
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }

        @Override
        public Executor asNonBlocking() {
            return ForkJoinPool.commonPool();
        }

    }
}
