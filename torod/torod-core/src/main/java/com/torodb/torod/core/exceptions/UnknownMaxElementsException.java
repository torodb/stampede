
package com.torodb.torod.core.exceptions;

import com.torodb.torod.core.cursors.ToroCursor;

/**
 * This exception is thrown when {@linkplain ToroCursor#getMaxElements()} is
 * called on a cursor whose max number of elements cannot be known.
 */
public class UnknownMaxElementsException extends ToroException {
    private static final long serialVersionUID = 1L;

    public UnknownMaxElementsException() {
    }

    public UnknownMaxElementsException(String message) {
        super(message);
    }

    public UnknownMaxElementsException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownMaxElementsException(Throwable cause) {
        super(cause);
    }

}
