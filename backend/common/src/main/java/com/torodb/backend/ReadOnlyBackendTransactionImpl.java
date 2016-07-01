
package com.torodb.backend;

import com.torodb.core.backend.ReadOnlyBackendTransaction;
import com.torodb.core.d2r.R2DTranslator;

/**
 *
 */
public class ReadOnlyBackendTransactionImpl extends BackendTransactionImpl implements ReadOnlyBackendTransaction {

    public ReadOnlyBackendTransactionImpl(SqlInterface sqlInterface, BackendConnectionImpl backendConnection,
            R2DTranslator r2dTranslator) {
        super(sqlInterface.getDbBackend().createReadOnlyConnection(), sqlInterface, backendConnection, r2dTranslator);
    }

}
