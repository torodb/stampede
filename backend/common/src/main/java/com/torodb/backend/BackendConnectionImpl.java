
package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.backend.ReadOnlyBackendTransaction;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.d2r.RidGenerator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BackendConnectionImpl implements BackendConnection {

    private static final Logger LOGGER = LogManager.getLogger(BackendConnectionImpl.class);
    private final BackendImpl backend;
    private final SqlInterface sqlInterface;
    private boolean closed = false;
    private final R2DTranslator r2dTranslator;
    private final IdentifierFactory identifierFactory;
    private final RidGenerator ridGenerator;
    private BackendTransaction currentTransaction;

    public BackendConnectionImpl(BackendImpl backend, SqlInterface sqlInterface, R2DTranslator r2dTranslator, IdentifierFactory identifierFactory, RidGenerator ridGenerator) {
        this.backend = backend;
        this.sqlInterface = sqlInterface;
        this.r2dTranslator = r2dTranslator;
        this.identifierFactory = identifierFactory;
        this.ridGenerator = ridGenerator;
    }

    @Override
    public ReadOnlyBackendTransaction openReadOnlyTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);
        
        ReadOnlyBackendTransactionImpl transaction = new ReadOnlyBackendTransactionImpl(sqlInterface, this, r2dTranslator);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public SharedWriteBackendTransaction openSharedWriteTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        SharedWriteBackendTransactionImpl transaction = new SharedWriteBackendTransactionImpl(sqlInterface, this, r2dTranslator);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public ExclusiveWriteBackendTransaction openExclusiveWriteTransaction() {
        Preconditions.checkState(!closed, "This connection is closed");
        Preconditions.checkState(currentTransaction == null, "Another transaction is currently under execution. Transaction is " + currentTransaction);

        ExclusiveWriteBackendTransactionImpl transaction = new ExclusiveWriteBackendTransactionImpl(sqlInterface, this, r2dTranslator, identifierFactory, ridGenerator);
        currentTransaction = transaction;

        return transaction;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (currentTransaction != null) {
                currentTransaction.close();
            }
            assert currentTransaction == null;
            backend.onConnectionClosed(this);
        }
    }

    void onTransactionClosed(BackendTransaction transaction) {
        if (currentTransaction == null) {
            LOGGER.debug("Recived an on transaction close notification, but there is no current transaction");
            return ;
        }
        if (currentTransaction != transaction) {
            LOGGER.debug("Recived an on transaction close notification, but the recived transaction is not the same as the current one");
            return ;
        }
        currentTransaction = null;
    }

}
