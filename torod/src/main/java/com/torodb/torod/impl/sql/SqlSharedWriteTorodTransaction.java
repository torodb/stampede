
package com.torodb.torod.impl.sql;

import com.torodb.core.transaction.SharedWriteInternalTransaction;

/**
 *
 */
public class SqlSharedWriteTorodTransaction extends SqlWriteTorodTransaction<SharedWriteInternalTransaction> {

    public SqlSharedWriteTorodTransaction(SqlTorodConnection connection, boolean concurrent) {
        super(connection, concurrent);
    }

    @Override
    protected SharedWriteInternalTransaction createInternalTransaction(SqlTorodConnection connection) {
        return connection
                .getServer()
                .getInternalTransactionManager()
                .openSharedWriteTransaction(getConnection().getBackendConnection());
    }

}
