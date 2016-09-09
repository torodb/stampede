
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
class WriteMongodTransactionImpl extends MongodTransactionImpl implements WriteMongodTransaction {

    private final WriteTorodTransaction torodTransaction;
    private final CommandsExecutor<? super WriteMongodTransactionImpl> commandsExecutor;

    public WriteMongodTransactionImpl(MongodConnection connection, boolean concurrent) {
        super(connection);
        this.torodTransaction = connection.getTorodConnection().openWriteTransaction(concurrent);
        this.commandsExecutor = connection.getServer().getCommandsExecutorClassifier().getWriteCommandsExecutor();
    }

    @Override
    public WriteTorodTransaction getTorodTransaction() {
        return torodTransaction;
    }

    @Override
    protected <Arg, Result> Status<Result> executeProtected(Request req, Command<? super Arg, ? super Result> command, Arg arg) {
        return commandsExecutor.execute(req, command, arg, this);
    }

    @Override
    public void commit() throws RollbackException, UserException {
        torodTransaction.commit();
    }

}
