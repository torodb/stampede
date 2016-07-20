package com.torodb.mongodb.guice;

import com.google.inject.AbstractModule;
import com.torodb.mongodb.annotations.MongoDBLayer;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(Executor.class)
                .annotatedWith(MongoDBLayer.class)
                .toInstance(ForkJoinPool.commonPool());

    }
}
