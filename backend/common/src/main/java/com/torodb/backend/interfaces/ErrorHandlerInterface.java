package com.torodb.backend.interfaces;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;

import com.torodb.core.exceptions.user.UserException;

public interface ErrorHandlerInterface {
    public enum Context {
        unknown,
        ddl,
        fetch,
        insert,
        update,
        delete,
        commit
    }
    
    void handleRollbackException(Context context, SQLException sqlException);
    void handleRollbackException(Context context, DataAccessException dataAccessException);
    
    void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException;
    void handleUserAndRetryException(Context context, DataAccessException dataAccessException) throws UserException;
}
