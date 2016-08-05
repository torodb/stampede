
package com.torodb.packaging.guice;

import com.torodb.concurrent.guice.ForkJoinToroDbExecutorProvider;
import com.eightkdata.mongowp.annotations.MongoWP;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.concurrent.ToroDbExecutorService;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.annotations.ToroDbRunnableService;
import com.torodb.packaging.ExecutorsService;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    }

    @Provides @Singleton
    ExecutorsService createExecutorsService(ThreadFactory threadFactory, ToroDbExecutorService executorService) {
        return new DefaultExecutorsService(threadFactory, Collections.singleton(executorService));
    }

    private static class DefaultExecutorsService extends ThreadFactoryIdleService implements ExecutorsService {
        private static final Logger LOGGER = LogManager.getLogger(DefaultExecutorsService.class);
        private final Collection<ExecutorService> executorServices;

        public DefaultExecutorsService(@ToroDbIdleService ThreadFactory threadFactory,
                Collection<ExecutorService> executorServices) {
            super(threadFactory);
            this.executorServices = executorServices;
        }

        @Override
        protected void startUp() throws Exception {
            //Nothing to do
        }

        @Override
        protected void shutDown() throws Exception {
            executorServices.stream().forEach((executorService) -> {
                executorService.shutdown();
            });
            for (ExecutorService executorService : executorServices) {
                if (!executorService.awaitTermination(100, TimeUnit.SECONDS)) {
                    LOGGER.warn("The executor service " + executorService + " did not terminate "
                            + "on the expected time");
                }
            }
        }
    }

}