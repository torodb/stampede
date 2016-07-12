
package com.torodb.torod;

import javax.annotation.Nonnull;
import javax.json.JsonObject;

/**
 *
 */
public class CollectionInfo {
    
    private static final String IS_CAPPED = "isCapped";
    private static final String MAX_IF_CAPPED = "maxIfCapped";
    
    private final String name;
    private final JsonObject properties;

    public CollectionInfo(@Nonnull String name, @Nonnull JsonObject properties) {
        this.name = name;
        this.properties = properties;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public JsonObject getProperties() {
        return properties;
    }

    public boolean isCapped() {
        return properties.containsKey(IS_CAPPED) && properties.getBoolean(IS_CAPPED);
    }

    public int getMaxIfCapped() {
        if (!properties.containsKey(MAX_IF_CAPPED)) {
            throw new IllegalStateException("The collection " + name + " has no " + MAX_IF_CAPPED + " property");
        }
        
        return properties.getInt(MAX_IF_CAPPED);
    }
}
