package com.torodb.poc.backend.interfaces;

import java.sql.SQLException;

import org.jooq.exception.DataAccessException;

import com.torodb.poc.backend.mocks.RetryTransactionException;

public interface ErrorHandlerInterface {
    public enum Context {
        unknown,
        select,
        insert,
        update,
        delete,
        batchInsert,
        batchUpdate,
        batchDelete,
        commit
    }
    
    void handleRetryException(Context context, SQLException sqlException) throws RetryTransactionException;
    void handleRetryException(Context context, DataAccessException sqlException) throws RetryTransactionException;
}
