
package com.torodb.mongodb.core;

import com.torodb.torod.ExclusiveWriteTorodTransaction;

/**
 *
 */
public interface ExclusiveWriteMongodTransaction extends WriteMongodTransaction {

    @Override
    public ExclusiveWriteTorodTransaction getTorodTransaction();

}
