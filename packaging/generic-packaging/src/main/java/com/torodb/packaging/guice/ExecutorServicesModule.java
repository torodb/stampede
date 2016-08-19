
package com.torodb.packaging.guice;

import com.eightkdata.mongowp.annotations.MongoWP;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.torodb.core.annotations.ParallelLevel;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.annotations.ToroDbRunnableService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
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
    }

}
