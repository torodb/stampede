
package com.torodb.core.transaction.metainf;

import java.util.Iterator;

import com.torodb.core.annotations.DoNotChange;

/**
 *
 */
public interface MutableMetaDocPartIndex extends MetaDocPartIndex {

    @Override
    public ImmutableMetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName);

    @Override
    public Iterator<? extends ImmutableMetaDocPartIndexColumn> iteratorColumns();

    /**
     * Adds a new column to this index.
     *
     * @param name
     * @param type
     * @param ordering
     * @return the new column
     * @throws IllegalArgumentException if this index already contains a column with the same
     *                                  {@link MetaDocPartIndexColumn#getName() name} and
     *                                  {@link MetaDocPartIndexColumn#getType() type}.
     */
    public abstract ImmutableMetaDocPartIndexColumn addMetaDocPartIndexColumn(String identifier, FieldIndexOrdering ordering) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaDocPartIndexColumn> getAddedMetaDocPartIndexColumns();

    public abstract ImmutableMetaDocPartIndex immutableCopy();
}
