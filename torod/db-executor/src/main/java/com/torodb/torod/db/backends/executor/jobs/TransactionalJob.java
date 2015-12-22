
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class TransactionalJob<R> extends Job<R> {

    private final DbConnection connection;
    private final TransactionAborter abortCallback;
    private final static Logger LOGGER = LoggerFactory.getLogger(TransactionalJob.class);

    public TransactionalJob(
            DbConnection connection, 
            TransactionAborter abortCallback) {
        this.connection = connection;
        this.abortCallback = abortCallback;
    }
    
    protected DbConnection getConnection() {
        return connection;
    }

    @Override
    protected final R onFail(Throwable t) throws ToroException, ToroRuntimeException {
        if (abortCallback.isAborted()) {
            throw new AbortedSessionTransactionException();
        }
        try {
            connection.rollback();
            abortCallback.abort(this);
        }
        catch (ImplementationDbException ex) {
            LOGGER.error("Error while rollbacking a transaction!");
            throw new ToroRuntimeException(t);
        }
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        if (t instanceof ToroRuntimeException) {
            throw (ToroRuntimeException) t;
        }
        throw new ToroRuntimeException(t);
    }
    
    public static interface TransactionAborter {
        
        public boolean isAborted();
        
        public <R> void abort(Job<R> job);
        
    }
    
    private static class AbortedSessionTransactionException extends ToroException {
        private static final long serialVersionUID = 1L;

        public AbortedSessionTransactionException() {
            super("This transaction has been aborted");
        }

    }

}
