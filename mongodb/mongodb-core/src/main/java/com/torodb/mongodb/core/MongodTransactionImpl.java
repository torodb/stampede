
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Preconditions;
import com.torodb.core.transaction.RollbackException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
abstract class MongodTransactionImpl implements MongodTransaction {
    private static final Logger LOGGER = LogManager.getLogger(MongodTransactionImpl.class);

    private final MongodConnection connection;
    private Request currentRequest;
    private boolean closed = false;

    MongodTransactionImpl(MongodConnection connection) {
        this.connection = connection;
    }

    protected abstract <Arg, Result> Status<Result> executeProtected(Request req, Command<? super Arg, ? super Result> command, Arg arg);

    @Override
    public MongodConnection getConnection() {
        return connection;
    }

    @Override
    public <Arg, Result> Status<Result> execute(Request req, Command<? super Arg, ? super Result> command, Arg arg) throws RollbackException {
        Preconditions.checkState(currentRequest == null, "Another request is currently under execution. Request is " + currentRequest);
        this.currentRequest = req;
        try {
            Status<Result> status = executeProtected(req, command, arg);
            return status;
        } finally {
            this.currentRequest = null;
        }
    }

    @Override
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
