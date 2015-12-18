
package com.torodb.di;

import com.google.inject.AbstractModule;
import com.torodb.torod.mongodb.annotations.MongoDBLayer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class ExecutorServiceModule extends AbstractModule {

    @Override
    protected void configure() {
        ExecutorService executorService = Executors.newCachedThreadPool(new MyThreadFactory());

        bind(ExecutorService.class).toInstance(executorService);

        bind(Executor.class).annotatedWith(MongoDBLayer.class).toInstance(executorService);
    }

    private static class MyThreadFactory implements ThreadFactory {

        private volatile long threadId = 0;

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "ToroDB-"+threadId++);
        }

    }

}
