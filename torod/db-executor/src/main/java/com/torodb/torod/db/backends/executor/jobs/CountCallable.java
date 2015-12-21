
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;

/**
 *
 */
public class CountCallable extends TransactionalJob<Integer> {

    private final String collection;
    private final QueryCriteria query;
    private final Report report;
    
    public CountCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report,
            String collection,
            QueryCriteria query) {
        super(connection, abortCallback);
        this.collection = collection;
        this.query = query;
        this.report = report;
    }

    @Override
    protected Integer failableCall() throws ToroException, ToroRuntimeException {
        Integer count = getConnection().count(collection, query);
        report.countExecuted(collection, query, count);
        return count;
    }

    public static interface Report {
        public void countExecuted(String collection, QueryCriteria query, int count);
    }
}
