
package com.torodb.torod.impl.sql;

import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.ExclusiveWriteInternalTransaction;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.torod.ExclusiveWriteTorodTransaction;

/**
 *
 */
public class SqlExclusiveWriteTorodTransaction extends SqlWriteTorodTransaction<ExclusiveWriteInternalTransaction> implements ExclusiveWriteTorodTransaction {

    public SqlExclusiveWriteTorodTransaction(SqlTorodConnection connection, boolean concurrent) {
        super(connection, concurrent);
    }

    @Override
    protected ExclusiveWriteInternalTransaction createInternalTransaction(SqlTorodConnection connection) {
        return connection
                .getServer()
                .getInternalTransactionManager()
                .openExclusiveWriteTransaction(getConnection().getBackendConnection());
    }

    @Override
    public void renameCollection(String fromDb, String fromCollection, String toDb, String toCollection) throws RollbackException, UserException {
        MutableMetaDatabase fromMetaDb = getMetaDatabaseOrThrowException(fromDb);
        MetaCollection fromMetaColl = getMetaCollectionOrThrowException(fromMetaDb, fromCollection);

        MutableMetaDatabase toMetaDb = getOrCreateMetaDatabase(toDb);
        MutableMetaCollection toMetaColl = createMetaCollection(toMetaDb, toCollection);
        
        getInternalTransaction().getBackendTransaction().renameCollection(fromMetaDb, fromMetaColl, toMetaDb, toMetaColl);

        fromMetaDb.removeMetaCollectionByName(fromCollection);
    }

}
