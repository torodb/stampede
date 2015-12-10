
package com.torodb.torod.db.backends.exceptions;

/**
 *
 */
public class InvalidCollectionSchemaException extends InvalidDatabaseException {
    private static final long serialVersionUID = 1L;

    private final String schemaName;

    public InvalidCollectionSchemaException(String schemaName) {
        this.schemaName = schemaName;
    }

    public InvalidCollectionSchemaException(String schemaName, String message) {
        super(message);
        this.schemaName = schemaName;
    }

    public InvalidCollectionSchemaException(String schemaName, String message, Throwable cause) {
        super(message, cause);
        this.schemaName = schemaName;
    }

    public InvalidCollectionSchemaException(String schemaName, Throwable cause) {
        super(cause);
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    
}
