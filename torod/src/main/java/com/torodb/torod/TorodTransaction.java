
package com.torodb.torod;

/**
 *
 */
public interface TorodTransaction extends AutoCloseable {

    @Override
    public void close();

}
