
package com.torodb.core.transaction.metainf;

import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaField implements MetaField {

    private final String name;
    private final String identifier;
    private final FieldType type;

    public ImmutableMetaField(String name, String identifier, FieldType type) {
        this.name = name;
        this.identifier = identifier;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public FieldType getType() {
        return type;
    }

}
