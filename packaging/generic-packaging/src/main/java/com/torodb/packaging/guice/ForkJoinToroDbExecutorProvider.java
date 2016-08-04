
package com.torodb.packaging.guice;

import com.torodb.core.ToroDbExecutorService;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.TimeUnit;
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

    private static class ForkJoinToroDbExecutor extends AbstractExecutorService implements ToroDbExecutorService {
        private final ForkJoinPool delegate;

        public ForkJoinToroDbExecutor(ForkJoinPool delegate) {
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
            delegate.execute(new MyManagedBlocker(command));
        }

        @Override
        public Executor asNonBlocking() {
            return delegate;
        }

    }

    private static class MyManagedBlocker implements ManagedBlocker, Runnable {

        private final Runnable runnable;
        private volatile boolean executed = false;

        public MyManagedBlocker(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public boolean block() throws InterruptedException {
            runnable.run();
            executed = true;
            return true;
        }

        @Override
        public boolean isReleasable() {
            return executed;
        }

        @Override
        public void run() {
            try {
                ForkJoinPool.managedBlock(this);
            } catch (InterruptedException ex) {
                Thread.interrupted();
                throw new RuntimeException(ex);
            }
        }

    }
}
