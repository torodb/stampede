
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.db.backends.executor.jobs.TransactionalJob;

/**
 *
 */
public class DropPathViewsCallable extends TransactionalJob<Void> {

    private final Report report;
    private final String collection;

    public DropPathViewsCallable(
            DbConnection connection,
            TransactionAborter abortCallback,
            Report report,
            String collection) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
    }

    @Override
    protected Void failableCall() throws ToroException,
            ToroRuntimeException {

        getConnection().dropPathViews(collection);
        report.dropViewsExecuted(collection);

        return null;
    }

    public static interface Report {
        public void dropViewsExecuted(
            String collection);
    }
}
