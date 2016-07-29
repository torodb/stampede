
package com.torodb.packaging.guice;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.torodb.mongodb.guice.MongoDbLayer;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.packaging.ExecutorsService;
import com.torodb.torod.guice.TorodLayer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class ExecutorServicesModule extends AbstractModule {

    @Override
    protected void configure() {
        ExecutorService torodbDefaultThreadPool = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setNameFormat("torodb-%d")
                .build()
        );

        bind(ExecutorService.class)
                .annotatedWith(TorodLayer.class)
                .toInstance(torodbDefaultThreadPool);

        bind(ExecutorService.class)
                .annotatedWith(MongoDbLayer.class)
                .toInstance(torodbDefaultThreadPool);

        bind(ExecutorService.class)
                .annotatedWith(MongoDbRepl.class)
                .toInstance(torodbDefaultThreadPool);

        bind(ExecutorsService.class)
                .toInstance(new DefaultExecutorsService(
                        Collections.singletonList(torodbDefaultThreadPool))
                );

    }

    private static class DefaultExecutorsService extends AbstractIdleService implements ExecutorsService {
        private static final Logger LOGGER = LogManager.getLogger(DefaultExecutorsService.class);
        private final Collection<ExecutorService> executorServices;

        public DefaultExecutorsService(Collection<ExecutorService> executorServices) {
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
                if (executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.warn("The executor service " + executorService + " did not terminate "
                            + "on the expected time");
                }
            }
        }
    }

}
