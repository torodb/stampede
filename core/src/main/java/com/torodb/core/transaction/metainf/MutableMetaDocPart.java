
package com.torodb.core.transaction.metainf;

import com.torodb.core.annotations.DoNotChange;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaDocPart extends MetaDocPart {

    @Override
    public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type);

    @Override
    public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String fieldName);

    @Override
    public ImmutableMetaField getMetaFieldByIdentifier(String fieldId);

    @Override
    public Stream<? extends ImmutableMetaField> streamFields();

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
    public abstract ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaField> getAddedMetaFields();

    public abstract ImmutableMetaDocPart immutableCopy();
}
