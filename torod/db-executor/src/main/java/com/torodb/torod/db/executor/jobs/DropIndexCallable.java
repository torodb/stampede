
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;

/**
 *
 */
public class DropIndexCallable extends SystemDbCallable<Boolean> {

    private final String indexName;

    public DropIndexCallable(DbWrapper dbWrapperPool, String indexName) {
        super(dbWrapperPool);
        this.indexName = indexName;
    }

    @Override
    Boolean call(DbConnection db) 
            throws ImplementationDbException, UserDbException {
        return db.dropIndex(indexName);
    }

    @Override
    void doCallback(Boolean result) {
    }

}
