
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.torod.ExclusiveWriteTorodTransaction;

/**
 *
 */
class ExclusiveWriteMongodTransactionImpl extends MongodTransactionImpl implements ExclusiveWriteMongodTransaction {

    private final ExclusiveWriteTorodTransaction torodTransaction;
    private final CommandsExecutor<? super ExclusiveWriteMongodTransactionImpl> commandsExecutor;

    public ExclusiveWriteMongodTransactionImpl(MongodConnection connection, boolean concurrent) {
        super(connection);
        this.torodTransaction = connection.getTorodConnection().openExclusiveWriteTransaction(concurrent);
        this.commandsExecutor = connection.getServer().getCommandsExecutorClassifier().getExclusiveWriteCommandsExecutor();
    }

    @Override
    public ExclusiveWriteTorodTransaction getTorodTransaction() {
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
