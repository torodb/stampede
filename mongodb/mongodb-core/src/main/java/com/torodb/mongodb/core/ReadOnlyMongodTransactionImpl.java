
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.torod.ReadOnlyTorodTransaction;

/**
 *
 */
class ReadOnlyMongodTransactionImpl extends MongodTransactionImpl implements ReadOnlyMongodTransaction {

    private final ReadOnlyTorodTransaction torodTransaction;
    private final CommandsExecutor<? super ReadOnlyMongodTransactionImpl> commandsExecutor;

    public ReadOnlyMongodTransactionImpl(MongodConnection connection) {
        super(connection);
        this.torodTransaction = connection.getTorodConnection().openReadOnlyTransaction();
        this.commandsExecutor = connection.getServer().getCommandsExecutorClassifier().getReadOnlyCommandsExecutor();
    }

    @Override
    public ReadOnlyTorodTransaction getTorodTransaction() {
        return torodTransaction;
    }
    @Override
    protected <Arg, Result> Status<Result> executeProtected(Request req, Command<? super Arg, ? super Result> command, Arg arg) {
        return commandsExecutor.execute(req, command, arg, this);
    }

}
