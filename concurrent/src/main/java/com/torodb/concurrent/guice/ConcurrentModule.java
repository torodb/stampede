
package com.torodb.concurrent.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.torodb.concurrent.AkkaStreamExecutor;
import com.torodb.core.concurrent.StreamExecutor;
import com.torodb.core.concurrent.ToroDbExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 *
 */
public class ConcurrentModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ToroDbExecutorService.class)
                .toProvider(ForkJoinToroDbExecutorProvider.class)
                .in(Singleton.class);

        bind(StreamExecutor.class)
                .to(AkkaStreamExecutor.class)
                .in(Singleton.class);
    }

}
