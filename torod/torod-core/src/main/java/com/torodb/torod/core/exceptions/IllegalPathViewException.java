
package com.torodb.torod.core.exceptions;

/**
 *
 */
public class IllegalPathViewException extends ToroException {

    private static final long serialVersionUID = 1L;

    public IllegalPathViewException() {
    }

    public IllegalPathViewException(String message) {
        super(message);
    }

    public IllegalPathViewException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalPathViewException(Throwable cause) {
        super(cause);
    }

}
