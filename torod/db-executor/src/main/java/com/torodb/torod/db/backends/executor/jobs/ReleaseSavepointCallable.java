
package com.torodb.torod.db.backends.executor.jobs;

import java.sql.Savepoint;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class ReleaseSavepointCallable extends TransactionalJob<Void> {

    private final Savepoint savepoint;
    private final Report report;
    
    public ReleaseSavepointCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report,
            Savepoint savepoint) {
        super(connection, abortCallback);
        this.savepoint = savepoint;
        this.report = report;
    }

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        try {
            getConnection().releaseSavepoint(savepoint);
            report.releaseSavepointExecuted();
            return null;
        } catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
    }

    public static interface Report {
        public void releaseSavepointExecuted();
    }
}
