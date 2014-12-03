
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.core.pojos.DefaultNamedToroIndex;
import com.torodb.torod.core.pojos.NamedToroIndex;
import com.torodb.torod.core.pojos.IndexedAttributes;

/**
 *
 */
public class CreateIndexCallable extends SystemDbCallable<NamedToroIndex> {

    private final String collectionName;
    private final String indexName;
    private final IndexedAttributes attributes;
    private final boolean unique;
    private final boolean blocking;
    private final SystemExecutor.CreateIndexCallback callback;

    public CreateIndexCallable(
            DbWrapper dbWrapperPool,
            String collectionName,
            String indexName, 
            IndexedAttributes attributes, 
            boolean unique, 
            boolean blocking, 
            SystemExecutor.CreateIndexCallback callback) {
        super(dbWrapperPool);
        this.collectionName = collectionName;
        this.indexName = indexName;
        this.attributes = attributes;
        this.unique = unique;
        this.blocking = blocking;
        this.callback = callback;
    }

    @Override
    NamedToroIndex call(DbConnection db) 
            throws ImplementationDbException, UserDbException {
        return db.createIndex(collectionName, indexName, attributes, unique, blocking);
    }

    @Override
    void doCallback(NamedToroIndex result) {
        callback.createdIndex(result);
    }
    

}
