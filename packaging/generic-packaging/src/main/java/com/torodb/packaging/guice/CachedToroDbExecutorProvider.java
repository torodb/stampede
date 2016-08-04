
package com.torodb.packaging.guice;

import com.torodb.concurrent.CachedToroDbExecutor;
import com.torodb.concurrent.ToroDbExecutorService;
import java.util.List;
import java.util.concurrent.*;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 *
 */
public class CachedToroDbExecutorProvider implements Provider<ToroDbExecutorService>{

    private final ThreadFactory threadFactory;

    @Inject
    public CachedToroDbExecutorProvider(ThreadFactory threadFactory) {
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
        return new CachedToroDbExecutor(actualExecutor);
    }

}
