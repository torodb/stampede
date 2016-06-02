
package com.torodb.backend.exceptions;

/**
 *
 */
public class InvalidDatabaseSchemaException extends InvalidDatabaseException {
    private static final long serialVersionUID = 1L;

    private final String schemaName;

    public InvalidDatabaseSchemaException(String schemaName) {
        this.schemaName = schemaName;
    }

    public InvalidDatabaseSchemaException(String schemaName, String message) {
        super(message);
        this.schemaName = schemaName;
    }

    public InvalidDatabaseSchemaException(String schemaName, String message, Throwable cause) {
        super(message, cause);
        this.schemaName = schemaName;
    }

    public InvalidDatabaseSchemaException(String schemaName, Throwable cause) {
        super(cause);
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    
}
