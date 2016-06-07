package com.torodb.backend.d2r.model;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.torodb.core.TableRef;

public class TableRefImpl extends TableRef {
    private final static TableRefImpl ROOT = new TableRefImpl();
    
    private final Optional<TableRef> parent;
    private final String name;
    
    public static TableRefImpl createRoot() {
        return ROOT;
    }
    
    public TableRefImpl createChild(String name) {
        return new TableRefImpl(this, name);
    }
    
    private TableRefImpl() {
        super();
        this.parent = Optional.empty();
        this.name = "";
    }
    
    private TableRefImpl(@Nonnull TableRef parent, @Nonnull String name) {
        super();
        this.parent = Optional.of(parent);
        this.name = name;
    }
    
    @Override
    public Optional<TableRef> getParent() {
        return parent;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((parent == null) ? 0 : parent.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableRefImpl other = (TableRefImpl) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (parent == null) {
            if (other.parent != null)
                return false;
        } else if (!parent.equals(other.parent))
            return false;
        return true;
    }
}
