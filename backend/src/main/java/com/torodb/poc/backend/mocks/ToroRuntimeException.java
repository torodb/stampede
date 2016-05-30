
package com.torodb.poc.backend.mocks;

/**
 *
 */
public class ToroRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ToroRuntimeException() {
    }

    public ToroRuntimeException(String message) {
        super(message);
    }

    public ToroRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ToroRuntimeException(Throwable cause) {
        super(cause);
    }

}
