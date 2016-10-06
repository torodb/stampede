
package com.torodb.torod;

import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
public interface TorodConnection extends AutoCloseable {

    public ReadOnlyTorodTransaction openReadOnlyTransaction();

    public SharedWriteTorodTransaction openWriteTransaction(boolean concurrent);

    public ExclusiveWriteTorodTransaction openExclusiveWriteTransaction(boolean concurrent);

    public int getConnectionId();

    public TorodServer getServer();

    @Override
    public void close();
}
