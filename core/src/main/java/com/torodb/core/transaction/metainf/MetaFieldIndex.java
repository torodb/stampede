
package com.torodb.core.transaction.metainf;

import javax.annotation.Nonnull;

/**
 *
 */
public interface MetaFieldIndex {

    @Nonnull
    public abstract int getPosition();
    /**
     * The name of the field to index on the document model.
     * @return
     */
    @Nonnull
    public abstract String getName();

    @Nonnull
    public abstract FieldType getType();

    @Nonnull
    public abstract FieldIndexOrdering getOrdering();

    public default String defautToString() {
        return "fieldIndex{" + "position:" + getPosition() + ", name:" + getName() + ", type:" + getType() + ", ordering:" + getOrdering() + '}';
    }
}
