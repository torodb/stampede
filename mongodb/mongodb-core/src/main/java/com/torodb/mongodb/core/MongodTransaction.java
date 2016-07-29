
package com.torodb.mongodb.core;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.torodb.core.transaction.RollbackException;
import com.torodb.torod.TorodTransaction;

/**
 *
 */
public interface MongodTransaction extends AutoCloseable {
    public TorodTransaction getTorodTransaction();

    public MongodConnection getConnection();

    public <Arg, Result> Status<Result> execute(Request req, Command<? super Arg, ? super Result> command, Arg arg) throws RollbackException;

    public Request getCurrentRequest();

    @Override
    public void close();

}
