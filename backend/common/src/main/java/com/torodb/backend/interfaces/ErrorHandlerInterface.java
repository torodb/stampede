package com.torodb.backend.interfaces;

import com.torodb.core.exceptions.user.UserException;
import java.sql.SQLException;
import org.jooq.exception.DataAccessException;

public interface ErrorHandlerInterface {
    public enum Context {
        unknown,
        get_connection,
        ddl,
        fetch,
        insert,
        update,
        delete,
        commit,
        close
    }
    
    void handleRollbackException(Context context, SQLException sqlException);
    void handleRollbackException(Context context, DataAccessException dataAccessException);
    
    void handleUserAndRetryException(Context context, SQLException sqlException) throws UserException;
    void handleUserAndRetryException(Context context, DataAccessException dataAccessException) throws UserException;
}
