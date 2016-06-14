package com.torodb.core.d2r;

import javax.annotation.Nonnull;

import com.torodb.kvdocument.values.KVValue;

public class FieldValue {
    public static final FieldValue NULL_VALUE = new FieldValue();
    
    private final KVValue<?> value;
    
    private FieldValue() {
        this.value = null;
    }
    
    public FieldValue(@Nonnull KVValue<?> value) {
        super();
        this.value = value;
    }

    public KVValue<?> getValue() {
        return value;
    }
}
