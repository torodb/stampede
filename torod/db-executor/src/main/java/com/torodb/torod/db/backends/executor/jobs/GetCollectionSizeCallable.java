
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class GetCollectionSizeCallable extends TransactionalJob<Long> {

    private final String collection;
    private final Report report;

    public GetCollectionSizeCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report,
            String collection) {
        super(connection, abortCallback);
        this.collection = collection;
        this.report = report;
    }

    @Override
    protected Long failableCall() throws ToroException, ToroRuntimeException {
        Long size = getConnection().getCollectionSize(collection);
        report.getCollectionSizeExecuted(collection, size);
        return size;
    }
    
    public static interface Report {
        public void getCollectionSizeExecuted(String collection, Long size);
    }
}
