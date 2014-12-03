package com.torodb.torod.db.sql.index;

import com.google.common.collect.ImmutableList;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class NamedDbIndex implements DbIndex {

    private final String name;
    private final UnnamedDbIndex unnamed;

    public NamedDbIndex(String name, UnnamedDbIndex unnamed) {
        this.name = name;
        this.unnamed = unnamed;
    }

    public String getName() {
        return name;
    }

    public UnnamedDbIndex getInfo() {
        return unnamed;
    }

    @Override
    public String getSchema() {
        return unnamed.getSchema();
    }

    @Override
    public boolean isUnique() {
        return unnamed.isUnique();
    }

    @Override
    public ImmutableList<IndexedColumnInfo> getColumns() {
        return unnamed.getColumns();
    }

    @Override
    public UnnamedDbIndex asUnnamed() {
        return unnamed;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (this.name != null ? this.name.hashCode() : 0);
        hash = 29 * hash + (this.unnamed != null ? this.unnamed.hashCode() : 0);
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
        final NamedDbIndex other = (NamedDbIndex) obj;
        if ((this.name == null) ? (other.name != null)
                : !this.name.equals(other.name)) {
            return false;
        }
        if (this.unnamed != other.unnamed && (this.unnamed == null ||
                !this.unnamed.equals(other.unnamed))) {
            return false;
        }
        return true;
    }

}
