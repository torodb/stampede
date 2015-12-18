
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class GetIndexSizeCallable extends TransactionalJob<Long> {

    private final Report report;
    private final String collection;
    private final String index;

    public GetIndexSizeCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report,
            String collection, 
            String index) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
        this.index = index;
    }

    @Override
    protected Long failableCall() throws ToroException, ToroRuntimeException {
        Long size = getConnection().getIndexSize(collection, index);
        report.getIndexSizeExecuted(collection, index, size);
        return size;
    }
    

    public static interface Report {
        public void getIndexSizeExecuted(String collection, String index, Long size);
    }
}
