
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.torod.WriteTorodTransaction;

/**
 *
 */
public class WriteMongodTransaction extends MongodTransaction {

    private final WriteTorodTransaction torodTransaction;
    private final CommandsExecutor<WriteMongodTransaction> commandsExecutor;

    public WriteMongodTransaction(MongodConnection connection) {
        super(connection);
        this.torodTransaction = connection.getTorodConnection().openWriteTransaction();
        this.commandsExecutor = connection.getServer().getWriteCommandsExecutor();
    }

    @Override
    public WriteTorodTransaction getTorodTransaction() {
        return torodTransaction;
    }

    @Override
    protected <Arg, Result> Status<Result> executeProtected(Request req, Command<? super Arg, ? super Result> command, Arg arg) {
        return commandsExecutor.execute(req, command, arg, this);
    }

    public void commit() throws RollbackException, UserException {
        torodTransaction.commit();
    }

}
