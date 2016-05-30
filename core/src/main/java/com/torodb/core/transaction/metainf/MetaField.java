
package com.torodb.core.transaction.metainf;

import javax.annotation.Nonnull;

/**
 *
 */
public interface MetaField {

    /**
     * The name of the given field on the document model.
     * @return
     */
    @Nonnull
    public abstract String getName();

    /**
     * The identifier of the given field on the SQL model
     * @return
     */
    @Nonnull
    public abstract String getIdentifier();

    @Nonnull
    public abstract FieldType getType();
}
