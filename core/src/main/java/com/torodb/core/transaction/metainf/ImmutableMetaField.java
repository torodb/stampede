
package com.torodb.core.transaction.metainf;

import com.torodb.kvdocument.types.KVType;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class ImmutableMetaField implements MetaField {

    private final String name;
    private final String identifier;
    private final KVType type;

    public ImmutableMetaField(String name, String identifier, KVType type) {
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
    public KVType getType() {
        return type;
    }

}
