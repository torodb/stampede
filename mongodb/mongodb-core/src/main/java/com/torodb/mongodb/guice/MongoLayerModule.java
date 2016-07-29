package com.torodb.mongodb.guice;

import com.google.inject.AbstractModule;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(Executor.class)
                .annotatedWith(MongoDbLayer.class)
                .toInstance(ForkJoinPool.commonPool());

    }
}
