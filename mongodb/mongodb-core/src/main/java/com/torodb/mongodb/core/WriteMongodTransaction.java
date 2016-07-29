
package com.torodb.mongodb.core;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.torod.WriteTorodTransaction;

/**
 *
 */
public interface WriteMongodTransaction extends MongodTransaction {

    @Override
    public WriteTorodTransaction getTorodTransaction();

    public void commit() throws RollbackException, UserException;

}
