
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class ActionAndCommitFuture<E, O> implements Future<E> {

    private final OpTime optime;
    private final Future<O> actionFuture;
    private final Future<?> commitFuture;

    public ActionAndCommitFuture(
            OpTime optime,
            Future<O> actionFuture,
            Future<?> commitFuture) {
        this.optime = optime;
        this.actionFuture = actionFuture;
        this.commitFuture = commitFuture;
    }

    OpTime getOptime() {
        return optime;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return actionFuture.cancel(mayInterruptIfRunning) && commitFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return actionFuture.isCancelled() || commitFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
        return commitFuture.isDone() && actionFuture.isDone();
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        commitFuture.get();
        return transform(actionFuture.get());
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("Not supported."); //TODO: Decide how to implement the timeout here
    }

    public abstract E transform(O actionResult);

}
