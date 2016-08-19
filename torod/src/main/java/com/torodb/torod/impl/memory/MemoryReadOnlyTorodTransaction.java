
package com.torodb.torod.impl.memory;

import com.torodb.torod.ReadOnlyTorodTransaction;
import com.torodb.torod.impl.memory.MemoryData.MDTransaction;

/**
 *
 */
public class MemoryReadOnlyTorodTransaction extends MemoryTorodTransaction implements ReadOnlyTorodTransaction {

    private final MemoryData.MDReadTransaction trans;

    public MemoryReadOnlyTorodTransaction(MemoryTorodConnection connection) {
        super(connection);
        trans = connection.getServer().getData().openReadTransaction();
    }

    @Override
    public void rollback() {
    }

    @Override
    protected MDTransaction getTransaction() {
        return trans;
    }

}
