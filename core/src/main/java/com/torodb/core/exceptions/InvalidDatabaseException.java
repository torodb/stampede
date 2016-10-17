
package com.torodb.core.exceptions;

/**
 *
 */
public class InvalidDatabaseException extends SystemException {
    private static final long serialVersionUID = 1L;

    public InvalidDatabaseException() {
    }

    public InvalidDatabaseException(String message) {
        super(message);
    }

    public InvalidDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidDatabaseException(Throwable cause) {
        super(cause);
    }

}
