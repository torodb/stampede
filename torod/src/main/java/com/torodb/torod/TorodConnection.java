
package com.torodb.torod;

import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
public interface TorodConnection extends AutoCloseable {

    public ReadOnlyTorodTransaction openReadOnlyTransaction();

    public WriteTorodTransaction openWriteTransaction(boolean concurrent);

    public int getConnectionId();

    public TorodServer getServer();

    @Override
    public void close();
}
