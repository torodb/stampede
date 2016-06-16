
package com.torodb.torod;

/**
 *
 */
public class ReadOnlyTorodTransaction implements TorodTransaction {

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
