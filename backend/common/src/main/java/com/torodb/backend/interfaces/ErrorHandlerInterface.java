package com.torodb.backend.interfaces;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;

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
    
    void handleRetryException(Context context, SQLException sqlException);
    void handleRetryException(Context context, DataAccessException sqlException);
}
