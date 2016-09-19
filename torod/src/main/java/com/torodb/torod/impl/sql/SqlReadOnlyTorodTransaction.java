
package com.torodb.torod.impl.sql;

import com.torodb.core.transaction.ReadOnlyInternalTransaction;
import com.torodb.torod.ReadOnlyTorodTransaction;

/**
 *
 */
public class SqlReadOnlyTorodTransaction extends SqlTorodTransaction<ReadOnlyInternalTransaction> implements ReadOnlyTorodTransaction {

    public SqlReadOnlyTorodTransaction(SqlTorodConnection connection) {
        super(connection);
    }

    @Override
    protected ReadOnlyInternalTransaction createInternalTransaction(SqlTorodConnection connection) {
        return connection
                .getServer()
                .getInternalTransactionManager()
                .openReadTransaction(getConnection().getBackendConnection());
    }

}
