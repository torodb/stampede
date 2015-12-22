
package com.torodb.torod.db.backends.exceptions;

/**
 *
 */
public class InvalidTableException extends InvalidDatabaseException {
    private static final long serialVersionUID = 1L;

    private final String schema;
    private final String table;

    public InvalidTableException(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public InvalidTableException(String schema, String table, String message) {
        super(message);
        this.schema = schema;
        this.table = table;
    }

    public InvalidTableException(String schema, String table, String message, Throwable cause) {
        super(message, cause);
        this.schema = schema;
        this.table = table;
    }

    public InvalidTableException(String schema, String table, Throwable cause) {
        super(cause);
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }
}
