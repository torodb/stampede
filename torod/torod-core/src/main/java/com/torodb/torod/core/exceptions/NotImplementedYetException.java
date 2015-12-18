
package com.torodb.torod.core.exceptions;

/**
 *
 */
public class NotImplementedYetException extends ToroRuntimeException {
    private static final long serialVersionUID = 1L;

    public NotImplementedYetException() {
    }

    public NotImplementedYetException(String message) {
        super(message);
    }

    public NotImplementedYetException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotImplementedYetException(Throwable cause) {
        super(cause);
    }

}
