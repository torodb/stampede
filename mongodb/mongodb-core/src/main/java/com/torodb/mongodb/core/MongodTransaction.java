
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Preconditions;
import com.torodb.torod.TorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public abstract class MongodTransaction implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(MongodTransaction.class);

    private final MongodConnection connection;
    private Request currentRequest;
    private boolean closed = false;

    MongodTransaction(MongodConnection connection) {
        this.connection = connection;
    }

    public abstract TorodTransaction getTorodTransaction();

    protected abstract <Arg, Result> Status<Result> executeProtected(Request req, Command<? super Arg, ? super Result> command, Arg arg);

    public MongodConnection getConnection() {
        return connection;
    }

    public <Arg, Result> Status<Result> execute(Request req, Command<? super Arg, ? super Result> command, Arg arg) {
        Preconditions.checkState(currentRequest == null, "Another request is currently under execution. Request is " + currentRequest);
        this.currentRequest = req;
        return executeProtected(req, command, arg);
    }

    public Request getCurrentRequest() {
        return currentRequest;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            getTorodTransaction().close();
            connection.onTransactionClosed(this);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            LOGGER.warn(this.getClass() + " finalized without being closed");
            close();
        }
    }

}
