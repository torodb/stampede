
package com.torodb.torod.db.backends.executor.jobs;

import java.util.List;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;

/**
 *
 */
public class ReadAllCallable extends TransactionalJob<List<ToroDocument>> {

    private final String collection;
    private final QueryCriteria query;
    private final Report report;
    
    public ReadAllCallable(
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
    protected List<ToroDocument> failableCall() throws ToroException, ToroRuntimeException {
        List<ToroDocument> docs = getConnection().readAll(collection, query);
        report.readAllExecuted(collection, query, docs);
        return docs;
    }

    public static interface Report {
        public void readAllExecuted(String collection, QueryCriteria query, List<ToroDocument> docs);
    }
}
