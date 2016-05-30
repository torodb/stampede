package com.torodb.core.impl;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.torodb.core.TableRef;

public class TableRefImpl extends TableRef {
    private final static TableRef ROOT = new TableRefImpl();
    
    private final Optional<TableRef> parent;
    private final String name;
    
    public static TableRef createRoot() {
        return ROOT;
    }
    
    public static TableRef createChild(TableRef tableRef, String name) {
        return new TableRefImpl(tableRef, name);
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
}
