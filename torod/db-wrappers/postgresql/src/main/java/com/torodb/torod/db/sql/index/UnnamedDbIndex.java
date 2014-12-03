
package com.torodb.torod.db.sql.index;

import com.google.common.collect.ImmutableList;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public final class UnnamedDbIndex implements DbIndex {
    private final String schema;
    private final boolean unique;
    private final ImmutableList<IndexedColumnInfo> columns;

    public UnnamedDbIndex(
            String schema, 
            boolean unique, 
            ImmutableList<IndexedColumnInfo> columns) {
        this.schema = schema;
        this.unique = unique;
        this.columns = columns;
    }
    
    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public ImmutableList<IndexedColumnInfo> getColumns() {
        return columns;
    }

    @Override
    public UnnamedDbIndex asUnnamed() {
        return this;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + (this.schema != null ? this.schema.hashCode() : 0);
        hash = 83 * hash + (this.unique ? 1 : 0);
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
        if (this.unique != other.unique) {
            return false;
        }
        return !(this.columns != other.columns &&
                (this.columns == null || !this.columns.equals(other.columns)));
    }

    public final static class Builder {

        private String schema;
        private boolean unique;
        private ImmutableList.Builder<IndexedColumnInfo> columnsBuilder;

        public Builder() {
            clear();
        }

        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder setUnique(boolean unique) {
            this.unique = unique;
            return this;
        }
        
        public Builder addColumn(IndexedColumnInfo columnInfo) {
            this.columnsBuilder.add(columnInfo);
            return this;
        }
        
        public void clear() {
            schema = null;
            unique = false;
            columnsBuilder = new ImmutableList.Builder<IndexedColumnInfo>();
        }

        public UnnamedDbIndex build() {
            return new UnnamedDbIndex(schema, unique, columnsBuilder.build());
        }

    }

}
