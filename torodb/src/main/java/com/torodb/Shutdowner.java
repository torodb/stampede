
package com.torodb;

import com.eightkdata.mongowp.mongoserver.MongoServer;
import com.google.inject.Provider;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.backend.DbBackend;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.ReplCoordinator.ReplCoordinatorOwnerCallback;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Singleton
public class Shutdowner implements ReplCoordinatorOwnerCallback {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(Shutdowner.class);
    private final Provider<DbBackend> dbBackend;
    private final Provider<Torod> torod;
    private final Provider<MongoServer> server;
    private final Provider<ReplCoordinator> replCoord;
    private final Provider<ExecutorService> executorService;

    @Inject
    public Shutdowner(Provider<DbBackend> dbBackend, Provider<Torod> torod, Provider<MongoServer> server, Provider<ReplCoordinator> replCoord, Provider<ExecutorService> executorService) {
        this.dbBackend = dbBackend;
        this.torod = torod;
        this.server = server;
        this.replCoord = replCoord;
        this.executorService = executorService;
    }

    public void shutdown() {
        server.get().stop();
        torod.get().shutdown();
        dbBackend.get().shutdown();
        ReplCoordinator replCoordinatorInstance = replCoord.get();
        replCoordinatorInstance.stopAsync();
        replCoordinatorInstance.awaitTerminated();

        shutdownExecutor();
    }

    @Override
    public void replCoordStopped() {
        server.get().stop();
        torod.get().shutdown();
        dbBackend.get().shutdown();
        
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        ExecutorService executorServiceInstance = executorService.get();
        executorServiceInstance.shutdown();
        boolean executorTerminated = false;
        try {
            executorTerminated = executorServiceInstance.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
        }

        if (!executorTerminated) {
            List<Runnable> notExecutedTasks = executorServiceInstance.shutdownNow();
            LOGGER.error("The executor service didn't stop in the expected time. {} task were waiting", notExecutedTasks);
        }
    }

}
