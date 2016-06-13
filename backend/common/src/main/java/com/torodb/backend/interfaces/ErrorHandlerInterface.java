package com.torodb.backend.interfaces;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;

import com.torodb.core.transaction.RollbackException;

public interface ErrorHandlerInterface {
    public enum Context {
        unknown,
        fetch,
        insert,
        update,
        delete,
        commit
    }
    
    void handleRetryException(Context context, SQLException sqlException) throws RollbackException;
    void handleRetryException(Context context, DataAccessException sqlException) throws RollbackException;
}
