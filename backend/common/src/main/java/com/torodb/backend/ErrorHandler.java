package com.torodb.backend;

import com.torodb.core.exceptions.user.UserException;
import java.sql.SQLException;
import org.jooq.exception.DataAccessException;

public interface ErrorHandler {
    public enum Context {
        UNKNOWN,
        GET_CONNECTION,
        CREATE_SCHEMA,
        CREATE_TABLE,
        ADD_COLUMN,
        CREATE_INDEX,
        DROP_SCHEMA,
        DROP_TABLE,
        DROP_INDEX,
        FETCH,
        META_INSERT,
        META_DELETE,
        INSERT,
        UPDATE,
        DELETE,
        COMMIT,
        CLOSE
    }
    
    void handleRollbackException(Context context, SQLException sqlException);
    void handleRollbackException(Context context, DataAccessException dataAccessException);
    
    void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException;
    void handleUserAndRetryException(Context context, DataAccessException dataAccessException) throws UserException;
}
