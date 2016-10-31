
package com.torodb.concurrent;

import com.torodb.core.Shutdowner;
import com.torodb.core.Shutdowner.ShutdownListener;
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
public class ExecutorServiceShutdownHelper {

    private static final Logger LOGGER = LogManager.getLogger(ExecutorServiceShutdownHelper.class);
    private final Shutdowner shutdowner;
    private final Clock clock;

    @Inject
    public ExecutorServiceShutdownHelper(Shutdowner shutdowner, Clock clock) {
        this.shutdowner = shutdowner;
        this.clock = clock;
    }

    public void terminateOnShutdown(String executorServiceName,
            ExecutorService executorService) {
        shutdowner.addShutdownListener(executorService,
                new ExecutorServiceShutdowner(executorServiceName));
    }

    public void shutdown(ExecutorService executorService) throws InterruptedException {
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

    private class ExecutorServiceShutdowner implements ShutdownListener<ExecutorService> {
        private final String executorServiceName;

        public ExecutorServiceShutdowner(String executorServiceName) {
            this.executorServiceName = executorServiceName;
        }

        @Override
        public void onShutdown(ExecutorService e) throws Exception {
            ExecutorServiceShutdownHelper.this.shutdown(e);
        }

        @Override
        public String describeResource(ExecutorService resource) {
            return executorServiceName + " executor service";
        }
    }

}
