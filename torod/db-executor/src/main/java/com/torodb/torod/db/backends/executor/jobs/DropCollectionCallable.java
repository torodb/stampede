
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;

/**
 *
 */
public class DropCollectionCallable extends SystemDbCallable<Void> {

    private final Report report;
    private final String collection;

    public DropCollectionCallable(DbWrapper dbWrapperPool, String collection, Report report) {
        super(dbWrapperPool);
        this.report = report;
        this.collection = collection;
    }
    
    @Override
    Void call(DbConnection db) throws ImplementationDbException {
        db.dropCollection(collection);

        return null;
    }

    @Override
    void doCallback(Void result) {
        report.dropCollectionExecuted(collection);
    }

    public static interface Report {
        public void dropCollectionExecuted(String collection);
    }
}
