
package com.torodb.torod.db.backends.sql.index;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public final class UnnamedDbIndex implements DbIndex {
    private static final long serialVersionUID = 1L;
    private final String schema;
    private final String table;
    private final String column;
    private final boolean ascending;

    public UnnamedDbIndex(
            String schema, 
            String tableName, 
            String columnName, 
            boolean ascending) {
        this.schema = schema;
        this.table = tableName;
        this.column = columnName;
        this.ascending = ascending;
    }
    
    @Override
    public String getSchema() {
        return schema;
    }
    
    @Override
    public String getTable() {
        return table;
    }
    
    @Override
    public String getColumn() {
        return column;
    }

    public boolean isAscending() {
        return ascending;
    }
    
    @Override
    public UnnamedDbIndex asUnnamed() {
        return this;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (this.schema != null ? this.schema.hashCode() : 0);
        hash = 29 * hash + (this.table != null ? this.table.hashCode() : 0);
        hash = 29 * hash + (this.column != null ? this.column.hashCode() : 0);
        hash = 29 * hash + (this.ascending ? 1 : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final UnnamedDbIndex other = (UnnamedDbIndex) obj;
        if ((this.schema == null) ? (other.schema != null)
                : !this.schema.equals(other.schema)) {
            return false;
        }
        if ((this.table == null) ? (other.table != null)
                : !this.table.equals(other.table)) {
            return false;
        }
        if ((this.column == null) ? (other.column != null)
                : !this.column.equals(other.column)) {
            return false;
        }
        return this.ascending == other.ascending;
    }

    @Override
    public String toString() {
        return "table=" + table + ", column=" + column +
                ", ascending=" + ascending;
    }

}
