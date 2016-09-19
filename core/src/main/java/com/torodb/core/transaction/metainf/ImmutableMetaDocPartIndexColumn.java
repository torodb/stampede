
package com.torodb.core.transaction.metainf;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaDocPartIndexColumn implements MetaDocPartIndexColumn {

    private final int position;
    private final String identifier;
    private final FieldIndexOrdering ordering;

    public ImmutableMetaDocPartIndexColumn(int position, String identifier, FieldIndexOrdering ordering) {
        this.position = position;
        this.identifier = identifier;
        this.ordering = ordering;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public String getIdentifier() {
        return identifier;
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
