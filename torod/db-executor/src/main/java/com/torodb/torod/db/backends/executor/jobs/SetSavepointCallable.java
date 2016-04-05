
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
public class SetSavepointCallable extends TransactionalJob<Savepoint> {

    private final Report report;
    
    public SetSavepointCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report) {
        super(connection, abortCallback);
        this.report = report;
    }

    @Override
    protected Savepoint failableCall() throws ToroException, ToroRuntimeException {
        try {
            Savepoint savepoint = getConnection().setSavepoint();
            report.setSavepointExecuted(savepoint);
            return savepoint;
        } catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
    }

    public static interface Report {
        public void setSavepointExecuted(Savepoint savepoint);
    }
}
