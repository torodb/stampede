
package com.torodb.torod.db.sql.index;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class IndexedColumnInfo {
    private final String tableName;
    private final String columnName;
    private final boolean ascending;

    public IndexedColumnInfo(String tableName, String columnName, boolean ascending) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.ascending = ascending;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isAscending() {
        return ascending;
    }

    @Override
    public String toString() {
        return tableName + '_' + columnName + (ascending ? 'a' : 'd');
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash
                = 97 * hash +
                (this.tableName != null ? this.tableName.hashCode() : 0);
        hash
                = 97 * hash +
                (this.columnName != null ? this.columnName.hashCode() : 0);
        hash = 97 * hash + (this.ascending ? 1 : 0);
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
        final IndexedColumnInfo other = (IndexedColumnInfo) obj;
        if ((this.tableName == null) ? (other.tableName != null)
                : !this.tableName.equals(other.tableName)) {
            return false;
        }
        if ((this.columnName == null) ? (other.columnName != null)
                : !this.columnName.equals(other.columnName)) {
            return false;
        }
        return this.ascending == other.ascending;
    }

}
