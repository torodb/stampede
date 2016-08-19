
package com.torodb.concurrent.guice;

import com.google.inject.Singleton;
import com.torodb.core.Shutdowner;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class ExecutorServiceShutdownHelper {

    private static final Logger LOGGER = LogManager.getLogger(ExecutorServiceShutdownHelper.class);
    private final Shutdowner shutdowner;
    private final Clock clock;

    @Inject
    public ExecutorServiceShutdownHelper(Shutdowner shutdowner, Clock clock) {
        this.shutdowner = shutdowner;
        this.clock = clock;
    }

    public void terminateOnShutdown(ExecutorService executorService) {
        shutdowner.addShutdownListener(executorService, this::onShutdown);
    }

    private void onShutdown(ExecutorService executorService) throws Exception {
        Instant start = clock.instant();
        executorService.shutdown();
        boolean terminated = false;
        int waitTime = 1;
        while (!terminated) {
            terminated = executorService.awaitTermination(waitTime, TimeUnit.SECONDS);
            if (!terminated) {
                LOGGER.info("ExecutorService {} did not finished in {}",
                        executorService,
                        Duration.between(start, clock.instant()));
            }
        }
    }

}
