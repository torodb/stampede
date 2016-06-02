
package com.torodb.backend.mocks;

/**
 *
 */
public class ImplementationDbException extends Exception {
    private static final long serialVersionUID = 1L;

    public ImplementationDbException() {
    }

    public ImplementationDbException(String message) {
        super(message);
    }

    public ImplementationDbException(String message, Throwable cause) {
        super(message, cause);
    }

    public ImplementationDbException(Throwable cause) {
        super(cause);
    }

}
