
package com.torodb.concurrent.guice;

import com.torodb.concurrent.CachedToroDbExecutor;
import com.torodb.core.concurrent.ToroDbExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class CachedToroDbExecutorProvider implements Provider<ToroDbExecutorService>{

    private final ThreadFactory threadFactory;
    private final ExecutorServiceShutdownHelper shutdownHelper;

    @Inject
    public CachedToroDbExecutorProvider(ThreadFactory threadFactory, 
            ExecutorServiceShutdownHelper ExecutorServiceShutdownHelper) {
        this.threadFactory = threadFactory;
        this.shutdownHelper = ExecutorServiceShutdownHelper;
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
        CachedToroDbExecutor result = new CachedToroDbExecutor(actualExecutor);
        shutdownHelper.terminateOnShutdown(result);

        return result;
    }

}
