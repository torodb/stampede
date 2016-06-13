package com.torodb.core.impl;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.torodb.core.TableRef;

public class TableRefImpl extends TableRef {
    private final static TableRef ROOT = new TableRefImpl();
    
    protected static TableRef createRoot() {
        return ROOT;
    }

    protected static TableRef createChild(TableRef parent, String name) {
        return new TableRefImpl(parent, name, false);
    }

    protected static TableRef createChild(TableRef parent, int arrayDepth) {
        String name = "$" + arrayDepth;
        name = name.intern();
        return new TableRefImpl(parent, name, true);
    }
    
    private final Optional<TableRef> parent;
    private final String name;
    private final int dimension;
    private final boolean isInArray;
    
    protected TableRefImpl() {
        super();
        this.parent = Optional.empty();
        this.name = "";
        this.dimension = 0;
        this.isInArray = false;
    }
    
    protected TableRefImpl(@Nonnull TableRef parent, @Nonnull String name, boolean isInArray) {
        super();
        this.parent = Optional.of(parent);
        this.name = name;
        this.dimension = parent.getDepth() + 1;
        this.isInArray = isInArray;
    }
    
    @Override
    public boolean isInArray() {
        return isInArray;
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
    public int getDepth() {
        return dimension;
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
