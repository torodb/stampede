package com.torodb.torod;

import com.torodb.core.language.AttributeReference;

public class IndexFieldInfo {
    private final AttributeReference attributeReference;
    private final boolean ascending;
    
    public IndexFieldInfo(AttributeReference attributeReference, boolean ascending) {
        super();
        this.attributeReference = attributeReference;
        this.ascending = ascending;
    }

    public AttributeReference getAttributeReference() {
        return attributeReference;
    }

    public boolean isAscending() {
        return ascending;
    }
}
