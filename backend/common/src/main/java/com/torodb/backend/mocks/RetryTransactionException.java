
package com.torodb.backend.mocks;

/**
 *
 */
public class RetryTransactionException extends Exception {
    private static final long serialVersionUID = 1L;

    public RetryTransactionException() {
    }

    public RetryTransactionException(String message) {
        super(message);
    }

    public RetryTransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryTransactionException(Throwable cause) {
        super(cause);
    }

}
