
package com.torodb.torod.core.pojos;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.json.JsonStructure;

/**
 *
 */
public class CollectionMetainfo {
    private final String name;
    private final boolean capped;
    private final int maxSize;
    private final int maxElements;
    private final JsonStructure json;
    private final String storageEngine;

    public CollectionMetainfo(
            String name, 
            boolean capped, 
            int maxSize, 
            int maxElements, 
            JsonStructure json, 
            String storageEngine) {
        this.name = name;
        this.capped = capped;
        this.maxSize = maxSize;
        this.maxElements = maxElements;
        this.json = json;
        this.storageEngine = storageEngine;
    }


    @Nonnull
    public String getName() {
        return name;
    }

    public boolean isCapped() {
        return capped;
    }

    @Nonnegative
    public int getMaxSize() {
        return maxSize;
    }

    @Nonnegative
    public int getMaxElements() {
        return maxElements;
    }

    public JsonStructure getJson() {
        return json;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + (this.name != null ? this.name.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CollectionMetainfo other = (CollectionMetainfo) obj;
        if ((this.name == null) ? (other.name != null)
                : !this.name.equals(other.name)) {
            return false;
        }
        return true;
    }
}
