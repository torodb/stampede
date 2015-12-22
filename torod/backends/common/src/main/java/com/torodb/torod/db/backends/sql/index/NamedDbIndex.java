package com.torodb.torod.db.backends.sql.index;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class NamedDbIndex implements DbIndex {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final UnnamedDbIndex unnamed;

    public NamedDbIndex(String name, UnnamedDbIndex unnamed) {
        this.name = name;
        this.unnamed = unnamed;
    }

    public String getName() {
        return name;
    }

    @Override
    public String getSchema() {
        return unnamed.getSchema();
    }

    @Override
    public String getTable() {
        return unnamed.getTable();
    }

    @Override
    public String getColumn() {
        return unnamed.getColumn();
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
        return !(this.unnamed != other.unnamed && (this.unnamed == null ||
                !this.unnamed.equals(other.unnamed)));
    }

}
