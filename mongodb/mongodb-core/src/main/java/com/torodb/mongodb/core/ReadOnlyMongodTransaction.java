
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.torod.ReadOnlyTorodTransaction;

/**
 *
 */
public class ReadOnlyMongodTransaction extends MongodTransaction {

    private final ReadOnlyTorodTransaction torodTransaction;
    private final CommandsExecutor<? super ReadOnlyMongodTransaction> commandsExecutor;

    public ReadOnlyMongodTransaction(MongodConnection connection) {
        super(connection);
        torodTransaction = connection.getTorodConnection().openReadOnlyTransaction();
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
