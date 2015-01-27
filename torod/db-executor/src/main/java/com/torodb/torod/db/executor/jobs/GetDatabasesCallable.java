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

    private final Report report;
    private final String databaseName;

    public GetDatabasesCallable(
            DbWrapper dbWrapperPool,
            Report report,
            String databaseName) {
        super(dbWrapperPool);
        this.report = report;
        this.databaseName = databaseName;
    }

    @Override
    List<? extends Database> call(DbConnection db)
            throws ImplementationDbException, UserDbException {
        List<DefaultDatabase> result = Collections.singletonList(
                new DefaultDatabase(
                        databaseName,
                        db.getDatabaseSize()
                )
        );
        return result;
    }

    @Override
    void doCallback(List<? extends Database> result) {
        report.getDatabasesExecuted(result);
    }

    public static interface Report {
        public void getDatabasesExecuted(List<? extends Database> databases);
    }
}
