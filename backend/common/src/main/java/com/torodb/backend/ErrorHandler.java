package com.torodb.backend;

import com.torodb.core.exceptions.ToroRuntimeException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;

import java.sql.SQLException;
import org.jooq.exception.DataAccessException;

public interface ErrorHandler {
    /**
     * The context of the backend error that
     * reflect the specific operation that is
     * performed when an error is received.
     */
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
    
    /**
     * Return the unchecked ToroRuntimeException exception that must be thrown. 
     * 
     * @param context
     * @param sqlException
     * @return an unchecked ToroRuntimeException
     * @throws RollbackException if the {@code sqlException} is due to a conflict resolvable by repeating the operation
     */
    ToroRuntimeException handleException(Context context, SQLException sqlException) throws RollbackException;
    
    /**
     * Return the unchecked ToroRuntimeException exception that must be thrown.
     * 
     * @param context
     * @param dataAccessException
     * @return an unchecked ToroRuntimeException
     * @throws RollbackException if the {@code dataAccessException} is due to a conflict resolvable by repeating the operation
     */
    ToroRuntimeException handleException(Context context, DataAccessException dataAccessException) throws RollbackException;
    
    /**
     * Return the unchecked ToroRuntimeException exception that must be thrown.
     * 
     * @param context
     * @param sqlException
     * @return
     * @throws UserException if the {@code sqlException} is due to an user mistake
     * @throws RollbackException if the {@code sqlException} is due to a conflict resolvable by repeating the operation
     */
    ToroRuntimeException handleUserException(Context context, SQLException sqlException) throws UserException, RollbackException;
    
    /**
     * Return the unchecked ToroRuntimeException exception that must be thrown.
     * 
     * @param context
     * @param dataAccessException
     * @return
     * @throws UserException if the {@code dataAccessException} is due to an user mistake
     * @throws RollbackException if the {@code dataAccessException} is due to a conflict resolvable by repeating the operation
     */
    ToroRuntimeException handleUserException(Context context, DataAccessException dataAccessException) throws UserException, RollbackException;
}
