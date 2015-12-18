
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class GetDocumentsSize extends TransactionalJob<Long> {
    
    private final Report report;
    private final String collection;

    public GetDocumentsSize(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report, 
            String collection) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
    }

    @Override
    protected Long failableCall() throws ToroException, ToroRuntimeException {
        Long size = getConnection().getDocumentsSize(collection);
        report.getDocumentSizeExecuted(collection, size);
        return size;
    }
    
    public static interface Report {
        public void getDocumentSizeExecuted(String collection, Long size);
    }
    
}
