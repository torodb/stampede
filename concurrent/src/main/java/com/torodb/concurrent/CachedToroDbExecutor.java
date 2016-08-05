
package com.torodb.concurrent;

import com.torodb.core.concurrent.ToroDbExecutorService;
import java.util.List;
import java.util.concurrent.*;

/**
 *
 */
public class CachedToroDbExecutor extends AbstractExecutorService implements ToroDbExecutorService {

    private final ThreadPoolExecutor delegate;

    public CachedToroDbExecutor(ThreadPoolExecutor delegate) {
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