
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import java.util.Collection;

/**
 *
 */
public class GetIndexesCallable extends TransactionalJob<Collection<? extends NamedToroIndex>> {

    private final Report report;
    private final String collection;

    public GetIndexesCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report, 
            String collection) {
        super(connection, abortCallback);
        this.report = report;
        this.collection = collection;
    }
    

    @Override
    protected Collection<? extends NamedToroIndex> failableCall() 
            throws ToroException, ToroRuntimeException {
        Collection<? extends NamedToroIndex> result
                = getConnection().getIndexes(collection);
        report.getIndexesExecuted(collection, result);
        
        return result;
    }
    
    public static interface Report {
        public void getIndexesExecuted(String collection, Collection<? extends NamedToroIndex> result);
    }

}
