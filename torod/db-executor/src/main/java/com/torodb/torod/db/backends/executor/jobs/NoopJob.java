
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class NoopJob extends Job<Void> {

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        return null;
    }

    @Override
    protected Void onFail(Throwable t) throws ToroException,
            ToroRuntimeException {
        throw new AssertionError("A noop job should never fail");
    }

}
