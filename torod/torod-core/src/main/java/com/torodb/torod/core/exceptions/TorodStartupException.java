package com.torodb.torod.core.exceptions;

public class TorodStartupException extends ToroRuntimeException {
    private static final long serialVersionUID = 1L;

    public TorodStartupException() {
    }

    public TorodStartupException(String message) {
        super(message);
    }

    public TorodStartupException(String message, Throwable cause) {
        super(message, cause);
    }

    public TorodStartupException(Throwable cause) {
        super(cause);
    }
    
}
