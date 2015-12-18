
package com.torodb.torod.mongodb.futures;

import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.*;

/**
 *
 */
public abstract class ActionAndCommitFuture<E, O> implements ListenableFuture<E> {

    private final OpTime optime;
    private final Future<O> actionFuture;
    private final Future<?> commitFuture;
    private boolean done = false, actionDone = false, commitDone = false;
    private final ExecutionList executionList = new ExecutionList();

    public ActionAndCommitFuture(
            OpTime optime,
            ListenableFuture<O> actionFuture,
            ListenableFuture<?> commitFuture) {
        this.optime = optime;
        this.actionFuture = actionFuture;
        this.commitFuture = commitFuture;

        Runnable myActionListener = new Runnable() {

            @Override
            public void run() {
                eventActionFinished();
            }
        };
        actionFuture.addListener(myActionListener, MoreExecutors.directExecutor());

        Runnable myCommitListener = new Runnable() {

            @Override
            public void run() {
                eventCommitFinished();
            }
        };
        commitFuture.addListener(myCommitListener, MoreExecutors.directExecutor());
    }

    public abstract E transform(O actionResult);

    OpTime getOptime() {
        return optime;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        executionList.add(listener, executor);
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

    private synchronized void eventActionFinished() {
        actionDone = true;
        if (!done) {
            if (commitDone) {
                eventDone();
            }
        }
    }

    private synchronized void eventCommitFinished() {
        commitDone = true;
        if (!done) {
            if (actionDone) {
                eventDone();
            }
        }
    }

    private void eventDone() {
        assert done == false;
        done = true;
        executionList.execute();
    }
}
