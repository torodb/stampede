
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class DropIndexCallable extends TransactionalJob<Boolean> {

    private final Report report;
    private final String collection;
    private final String indexName;

    public DropIndexCallable(
            DbConnection connection, 
            TransactionAborter abortCallback, 
            Report report, 
            String collection, 
            String indexName) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
        this.indexName = indexName;
    }

    @Override
    protected Boolean failableCall() throws ToroException, ToroRuntimeException {
        boolean result = getConnection().dropIndex(collection, indexName);
        report.dropIndexExecuted(collection, indexName, result);
        return result;
    }

    public static interface Report {
        public void dropIndexExecuted(String collection, String indexName, boolean removed);
    }
}
