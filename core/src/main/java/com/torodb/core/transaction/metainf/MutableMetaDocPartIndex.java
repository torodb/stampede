
package com.torodb.core.transaction.metainf;

import java.util.stream.Stream;

import com.torodb.core.annotations.DoNotChange;

/**
 *
 */
public interface MutableMetaDocPartIndex extends MetaDocPartIndex {

    @Override
    public ImmutableMetaFieldIndex getMetaFieldIndexByNameAndType(String fieldName, FieldType type);

    @Override
    public Stream<? extends ImmutableMetaFieldIndex> streamFields();

    /**
     * Adds a new column to this index.
     *
     * @param name
     * @param type
     * @return the new column
     * @throws IllegalArgumentException if this index already contains a column with the same
     *                                  {@link MetaFieldIndex#getName() name} and
     *                                  {@link MetaFieldIndex#getType() type}.
     */
    public abstract ImmutableMetaFieldIndex addMetaFieldIndex(String name, String identifier, FieldType type, FieldIndexOrdering ordering) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaFieldIndex> getAddedMetaFieldIndexes();

    public abstract ImmutableMetaDocPartIndex immutableCopy();
}
