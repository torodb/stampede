
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import java.util.concurrent.Callable;

/**
 *
 */
public abstract class Job<R> implements Callable<R> {

    protected abstract R failableCall() throws ToroException, ToroRuntimeException;
    
    protected abstract R onFail(Throwable t) throws ToroException, ToroRuntimeException;
    
    @Override
    public final R call() throws ToroException, ToroRuntimeException {
        try {
            return failableCall();
        } catch (Throwable t) {
            return onFail(t);
        }
    }

}
