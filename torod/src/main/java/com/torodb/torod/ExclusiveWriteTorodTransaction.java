
package com.torodb.torod;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;

/**
 *
 */
public interface ExclusiveWriteTorodTransaction extends SharedWriteTorodTransaction {
    
    public void renameCollection(String fromDb, String fromCollection, String toDb, String toCollection) throws RollbackException, UserException;

}
