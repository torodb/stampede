
package com.torodb.core.transaction.metainf;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaFieldIndex implements MetaFieldIndex {

    private final int position;
    private final String name;
    private final FieldType type;
    private final FieldIndexOrdering ordering;

    public ImmutableMetaFieldIndex(int position, String name, FieldType type, FieldIndexOrdering ordering) {
        this.position = position;
        this.name = name;
        this.type = type;
        this.ordering = ordering;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FieldType getType() {
        return type;
    }

    @Override
    public FieldIndexOrdering getOrdering() {
        return ordering;
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
