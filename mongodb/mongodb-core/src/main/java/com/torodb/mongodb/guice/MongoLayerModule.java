package com.torodb.mongodb.guice;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.torodb.mongodb.annotations.MongoDBLayer;

/**
 *
 */
public class MongoLayerModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(Executor.class)
                .annotatedWith(MongoDBLayer.class)
                .toInstance(Executors.newCachedThreadPool());

    }
}
