
package com.torodb.torod.core.exceptions;

/**
 *
 */
public class ClosedToroCursorException extends ToroException {
    private static final long serialVersionUID = 1L;

    public ClosedToroCursorException() {
    }

    public ClosedToroCursorException(String message) {
        super(message);
    }

    public ClosedToroCursorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClosedToroCursorException(Throwable cause) {
        super(cause);
    }

}
