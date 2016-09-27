
package com.torodb.packaging.guice;

import com.eightkdata.mongowp.annotations.MongoWP;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.torodb.concurrent.DefaultConcurrentToolsFactory;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction;
import com.torodb.concurrent.DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.annotations.ToroDbRunnableService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class ExecutorServicesModule extends AbstractModule {
    
    @Override
    protected void configure() {

        bind(Integer.class)
                .annotatedWith(ParallelLevel.class)
                .toInstance(Runtime.getRuntime().availableProcessors());

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("torodb-executor-%d")
                .build();

        bind(ThreadFactory.class)
                .toInstance(threadFactory);

        bind(ThreadFactory.class)
                .annotatedWith(ToroDbIdleService.class)
                .toInstance(threadFactory);

        bind(ThreadFactory.class)
                .annotatedWith(ToroDbRunnableService.class)
                .toInstance(threadFactory);

        bind(ThreadFactory.class)
                .annotatedWith(MongoWP.class)
                .toInstance(threadFactory);

        bind(ForkJoinWorkerThreadFactory.class)
                .toInstance(ForkJoinPool.defaultForkJoinWorkerThreadFactory);

        bind(DefaultConcurrentToolsFactory.BlockerThreadFactoryFunction.class)
                .toInstance(new CustomBlockerThreadFactoryFunction());

        bind(DefaultConcurrentToolsFactory.ForkJoinThreadFactoryFunction.class)
                .toInstance(new CustomForkJoinThreadFactoryFunction());
    }

    private static class CustomBlockerThreadFactoryFunction implements BlockerThreadFactoryFunction {
        @Override
        public ThreadFactory apply(String prefix) {
            return new ThreadFactoryBuilder()
                    .setNameFormat(prefix + " -%d")
                    .build();
        }
    }
    
    private static class CustomForkJoinThreadFactoryFunction implements ForkJoinThreadFactoryFunction {
        @Override
        public ForkJoinWorkerThreadFactory apply(String prefix) {
            return new CustomForkJoinThreadFactory(prefix);
        }
    }
    
    private static class CustomForkJoinThreadFactory implements ForkJoinWorkerThreadFactory {
        private final String prefix;
        private volatile int idProvider = 0;
        
        public CustomForkJoinThreadFactory(String prefix) {
            super();
            this.prefix = prefix;
        }

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            ForkJoinWorkerThread newThread = ForkJoinPool
                    .defaultForkJoinWorkerThreadFactory.newThread(pool);
            int id = idProvider++;
            newThread.setName(prefix + '-' + id);
            return newThread;
        }
    }

}
