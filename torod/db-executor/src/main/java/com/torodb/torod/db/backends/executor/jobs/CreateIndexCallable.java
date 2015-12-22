package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;

/**
 *
 */
public class CreateIndexCallable extends TransactionalJob<NamedToroIndex> {

    private final Report report;
    private final String collectionName;
    private final String indexName;
    private final IndexedAttributes attributes;
    private final boolean unique;
    private final boolean blocking;

    public CreateIndexCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report, 
            String collectionName, 
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking) {
        super(connection, abortCallback);
        this.report = report;
        this.collectionName = collectionName;
        this.indexName = indexName;
        this.attributes = attributes;
        this.unique = unique;
        this.blocking = blocking;
    }

    @Override
    protected NamedToroIndex failableCall() throws ToroException, ToroRuntimeException {
        NamedToroIndex result = getConnection().createIndex(
                collectionName,
                indexName,
                attributes,
                unique,
                blocking
        );
        report.createIndexExecuted(
                collectionName, 
                indexName, 
                attributes, 
                unique, 
                blocking, 
                result
        );

        return result;
    }

    public static interface Report {

        public void createIndexExecuted(
                String collectionName,
                String indexName,
                IndexedAttributes attributes,
                boolean unique,
                boolean blocking,
                NamedToroIndex result);
    }
}
