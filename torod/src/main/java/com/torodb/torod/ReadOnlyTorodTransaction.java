
package com.torodb.torod;

import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.ReadOnlyInternalTransaction;

/**
 *
 */
public class ReadOnlyTorodTransaction extends TorodTransaction {

    private final ReadOnlyInternalTransaction internalTransaction;

    public ReadOnlyTorodTransaction(TorodConnection connection) {
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
