
package com.torodb.torod.db.executor.jobs;

import com.google.common.base.Supplier;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import java.util.concurrent.Callable;

/**
 *
 */
public class DropCollectionCallable extends TransactionalJob<Void> {

    private final Report report;
    private final String collection;

    public DropCollectionCallable(
            DbConnection connection, 
            TransactionAborter abortCallback, 
            Report report, 
            String collection) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
    }

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        getConnection().dropCollection(collection);
        report.dropCollectionExecuted(collection);
        return null;
    }

    public static interface Report {
        public void dropCollectionExecuted(String collection);
    }
}
