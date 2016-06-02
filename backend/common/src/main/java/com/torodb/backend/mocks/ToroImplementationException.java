
package com.torodb.backend.mocks;

/**
 *
 */
public class ToroImplementationException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ToroImplementationException() {
    }

    public ToroImplementationException(String message) {
        super(message);
    }

    public ToroImplementationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ToroImplementationException(Throwable cause) {
        super(cause);
    }

}
