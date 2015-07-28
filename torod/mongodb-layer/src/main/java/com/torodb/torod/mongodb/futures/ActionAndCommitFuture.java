
package com.torodb.torod.mongodb.futures;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public abstract class ActionAndCommitFuture<E, O> implements Future<E> {

    private final Future<O> actionFuture;
    private final Future<?> commitFuture;

    public ActionAndCommitFuture(Future<O> actionFuture, Future<?> commitFuture) {
        this.actionFuture = actionFuture;
        this.commitFuture = commitFuture;
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
