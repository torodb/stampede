
package com.torodb.backend;

import com.torodb.core.backend.ReadOnlyBackendTransaction;

/**
 *
 */
public class ReadOnlyBackendTransactionImpl extends BackendTransactionImpl implements ReadOnlyBackendTransaction {

    public ReadOnlyBackendTransactionImpl(SqlInterface sqlInterface,
            BackendConnectionImpl backendConnection) {
        super(sqlInterface.getDbBackend().createReadOnlyConnection(),
                sqlInterface, backendConnection);
    }

}
