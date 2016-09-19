
package com.torodb.core.transaction.metainf;

import java.util.Iterator;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;

/**
 *
 */
public interface MutableMetaIndex extends MetaIndex {

    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position);

    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name);

    @Override
    public Iterator<? extends ImmutableMetaIndexField> iteratorFields();
    
    @Override
    public Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(TableRef tableRef);

    /**
     * Adds a new field to this index.
     *
     * @param name
     * @return the new field
     * @throws IllegalArgumentException if this index already contains a field with the same
     *                                  {@link MetaIndexField#getPosition() position} or with the same pair
     *                                  {@link MetaIndexField#getTableRef() tableRef} and
     *                                  {@link MetaIndexField#getName() name}.
     */
    public abstract ImmutableMetaIndexField addMetaIndexField(TableRef tableRef, String name, FieldIndexOrdering ordering) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaIndexField> getAddedMetaIndexFields();

    public abstract ImmutableMetaIndex immutableCopy();
}
