
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class Job<R> implements Callable<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    protected abstract R failableCall() throws ToroException, ToroRuntimeException;
    
    protected abstract R onFail(Throwable t) throws ToroException, ToroRuntimeException;
    
    @Override
    public final R call() throws ToroException, ToroRuntimeException {
        try {
            LOGGER.debug("Executing {}", this);
            R result = failableCall();
            LOGGER.debug("Executed {}", this);
            return result;
        } catch (Throwable t) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Error while executing " + this + ". Calling onFail()", t);
            }
            return onFail(t);
        }
    }

}
