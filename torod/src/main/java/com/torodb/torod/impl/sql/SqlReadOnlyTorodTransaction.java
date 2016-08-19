
package com.torodb.torod.impl.sql;

import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.ReadOnlyInternalTransaction;
import com.torodb.torod.ReadOnlyTorodTransaction;

/**
 *
 */
public class SqlReadOnlyTorodTransaction extends SqlTorodTransaction implements ReadOnlyTorodTransaction {

    private final ReadOnlyInternalTransaction internalTransaction;

    public SqlReadOnlyTorodTransaction(SqlTorodConnection connection) {
        super(connection);
        this.internalTransaction = connection
                .getServer()
                .getInternalTransactionManager()
                .openReadTransaction(getConnection().getBackendConnection());
    }

    @Override
    protected InternalTransaction getInternalTransaction() {
        return internalTransaction;
    }
}
