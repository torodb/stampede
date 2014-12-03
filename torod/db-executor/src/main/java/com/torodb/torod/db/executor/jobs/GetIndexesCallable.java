
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.pojos.NamedToroIndex;
import java.util.Collection;

/**
 *
 */
public class GetIndexesCallable extends SystemDbCallable<Collection<? extends NamedToroIndex>> {

    public GetIndexesCallable(DbWrapper dbWrapperPool) {
        super(dbWrapperPool);
    }

    @Override
    Collection<? extends NamedToroIndex> call(DbConnection db) 
            throws ImplementationDbException, UserDbException {
        return db.getIndexes();
    }

    @Override
    void doCallback(Collection<? extends NamedToroIndex> result) {
    }

}
