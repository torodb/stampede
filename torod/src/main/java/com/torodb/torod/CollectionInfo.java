
package com.torodb.torod;

import javax.annotation.Nonnull;
import javax.json.JsonObject;

/**
 *
 */
public class CollectionInfo {
    private final String name;
    private final JsonObject properties;

    public CollectionInfo(String name, JsonObject properties) {
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

}
