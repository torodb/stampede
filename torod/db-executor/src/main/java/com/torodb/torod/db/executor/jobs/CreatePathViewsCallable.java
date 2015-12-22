
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.db.backends.executor.jobs.TransactionalJob;

/**
 *
 */
public class CreatePathViewsCallable extends TransactionalJob<Integer> {

    private final Report report;
    private final String collection;

    public CreatePathViewsCallable(
            DbConnection connection,
            TransactionAborter abortCallback,
            Report report,
            String collection) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
    }

    @Override
    protected Integer failableCall() throws ToroException,
            ToroRuntimeException {

        Integer result = getConnection().createPathViews(collection);
        report.createViewsExecuted(collection, result);

        return result;
    }

    public static interface Report {
        public void createViewsExecuted(
            String collection,
            Integer result);
    }
}
