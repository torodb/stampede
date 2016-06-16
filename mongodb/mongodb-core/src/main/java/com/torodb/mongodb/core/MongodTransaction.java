
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Preconditions;
import com.torodb.torod.TorodTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public final class MongodTransaction implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(MongodTransaction.class);

    private final MongodConnection connection;
    private Request<?> currentRequest;
    private final CommandsExecutor executor;
    private boolean closed = false;

    public MongodTransaction(MongodConnection connection) {
        this.connection = connection;
        this.executor = connection.getServer().getCommandsExecutor();
    }

    public MongodConnection getConnection() {
        return connection;
    }
    
    public <C extends Command<? super A, ? super R>, A, R> Status<R> execute(C command, A arg, Request<MongodConnection> request) throws MongoException {
        Preconditions.checkState(currentRequest == null, "Another request is currently under execution. Request is " + currentRequest);
        this.currentRequest = request;
        return executor.execute(command, arg, request);
    }

    public abstract TorodTransaction getTorodTransaction();

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
