
package com.torodb.torod;

import com.torodb.core.transaction.RollbackException;

/**
 *
 */
public class TorodTransaction implements AutoCloseable {

    @Override
    public void close() throws RollbackException {
    }

}
