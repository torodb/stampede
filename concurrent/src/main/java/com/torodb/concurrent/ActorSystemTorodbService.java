
package com.torodb.concurrent;

import akka.actor.ActorSystem;
import akka.dispatch.ExecutionContexts;
import com.torodb.core.services.ExecutorTorodbService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/**
 *
 */
public abstract class ActorSystemTorodbService
        extends ExecutorTorodbService<ExecutorService> {

    private ActorSystem actorSystem;
    private final String actorSystemName;

    public ActorSystemTorodbService(ThreadFactory threadFactory,
            Function<ThreadFactory, ExecutorService> executorServiceProvider,
            String actorSystemName) {
        super(threadFactory, executorServiceProvider);
        this.actorSystemName = actorSystemName;
    }

    public ActorSystemTorodbService(ThreadFactory threadFactory,
            Supplier<ExecutorService> executorServiceSupplier,
            String actorSystemName) {
        super(threadFactory, executorServiceSupplier);
        this.actorSystemName = actorSystemName;
    }

    protected abstract Logger getLogger();

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        actorSystem = ActorSystem.create(actorSystemName, null, null,
                ExecutionContexts.fromExecutorService(getExecutorService())
        );
    }

    @Override
    protected void shutDown() throws Exception {
        if (actorSystem != null) {
            try {
                Await.result(actorSystem.terminate(), Duration.Inf());
            } catch (Exception ex) {
                throw new RuntimeException("It was impossible to shutdown the "
                        + "actor system with name " + actorSystemName, ex);
            }
        }
        getLogger().debug("Actor system with name {} terminated", actorSystemName);
        super.shutDown();
    }

}
