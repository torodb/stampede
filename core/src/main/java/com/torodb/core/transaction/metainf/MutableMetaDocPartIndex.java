
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
     * Put a new column to this index at specified position.
     *
     * @param position
     * @param identifier
     * @param ordering
     * @return the new column
     * @throws IllegalArgumentException if this index already contains a column with the same
     *                                  {@link MetaDocPartIndexColumn#getPosition() position} or
     *                                  {@link MetaDocPartIndexColumn#getName() identifier}.
     */
    public abstract ImmutableMetaDocPartIndexColumn putMetaDocPartIndexColumn(int position, String identifier, FieldIndexOrdering ordering) throws IllegalArgumentException;

    /**
     * Adds a new column to this index at next free position.
     *
     * @param identifier
     * @param ordering
     * @return the new column
     * @throws IllegalArgumentException if this index already contains a column with the same
     *                                  {@link MetaDocPartIndexColumn#getName() identifier}.
     */
    public abstract ImmutableMetaDocPartIndexColumn addMetaDocPartIndexColumn(String identifier, FieldIndexOrdering ordering) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaDocPartIndexColumn> getAddedMetaDocPartIndexColumns();

    /**
     * @throws IllegalArgumentException if this index does not contains all column from position 0 to the position for the column with maximum position
     */
    public abstract ImmutableMetaIdentifiedDocPartIndex immutableCopy(String identifier) throws IllegalArgumentException;
}
