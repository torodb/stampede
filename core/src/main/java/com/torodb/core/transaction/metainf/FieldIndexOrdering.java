package com.torodb.core.transaction.metainf;

public enum FieldIndexOrdering {
    ASC(true),
    DESC(false);
    
    private final boolean ascending;
    
    private FieldIndexOrdering(boolean ascending) {
        this.ascending = ascending;
    }
    
    public boolean isAscending() {
        return ascending;
    }
}
