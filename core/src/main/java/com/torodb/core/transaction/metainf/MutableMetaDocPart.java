
package com.torodb.core.transaction.metainf;

import com.torodb.kvdocument.types.KVType;

/**
 *
 */
public interface MutableMetaDocPart extends MetaDocPart<MetaField> {

    /**
     * Adds a new field to this table.
     *
     * @param name
     * @param identifier
     * @param type
     * @return the new column
     * @throws IllegalArgumentException if this table already contains a column with the same
     *                                  {@link DbColumn#getIdentifier() id} or with the same pair
     *                                  {@link DbColumn#getName() name} and
     *                                  {@link DbColumn#getType() type}.
     */
    public abstract ImmutableMetaField addMetaField(String name, String identifier, KVType type) throws IllegalArgumentException;

}
