package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.pojos.Database;
import com.torodb.torod.core.pojos.DefaultDatabase;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class GetDatabasesCallable extends TransactionalJob<List<? extends Database>> {

    private final Report report;
    private final String databaseName;

    public GetDatabasesCallable(
            DbConnection connection, 
            TransactionAborter abortCallback,
            Report report, 
            String databaseName) {
        super(connection, abortCallback);
        this.report = report;
        this.databaseName = databaseName;
    }

    @Override
    protected List<? extends Database> failableCall() throws ToroException,
            ToroRuntimeException {
        List<DefaultDatabase> result = Collections.singletonList(
                new DefaultDatabase(
                        databaseName,
                        getConnection().getDatabaseSize()
                )
        );
        report.getDatabasesExecuted(Collections.unmodifiableList(result));
        return result;
    }

    public static interface Report {
        public void getDatabasesExecuted(List<? extends Database> databases);
    }
}
