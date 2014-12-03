
package com.torodb.torod.db.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.DefaultDatabase;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class GetDatabasesCallable extends SystemDbCallable<List<? extends Database>> {

    private final String databaseName;

    public GetDatabasesCallable(
            DbWrapper dbWrapperPool,
            String databaseName) {
        super(dbWrapperPool);
        this.databaseName = databaseName;
    }

    @Override
    List<? extends Database> call(DbConnection db) 
            throws ImplementationDbException, UserDbException {
        return Collections.singletonList(
                new DefaultDatabase(
                        databaseName, 
                        db.getDatabaseSize()
                )
        );
    }

    @Override
    void doCallback(List<? extends Database> result) {
    }

}
