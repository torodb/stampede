
package com.torodb.core.transaction.metainf;

import javax.annotation.Nonnull;

/**
 *
 */
public interface MetaDocPartIndexColumn {

    @Nonnull
    public abstract int getPosition();
    /**
     * The name of the column to index on the database.
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    @Nonnull
    public abstract FieldIndexOrdering getOrdering();

    public default String defautToString() {
        return "fieldIndex{" + "position:" + getPosition() + ", identifier:" + getIdentifier() + ", ordering:" + getOrdering() + '}';
    }
}
