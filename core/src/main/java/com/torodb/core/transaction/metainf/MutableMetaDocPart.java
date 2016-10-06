
package com.torodb.core.transaction.metainf;

import java.util.List;
import java.util.stream.Stream;

import org.jooq.lambda.tuple.Tuple2;

import com.torodb.core.annotations.DoNotChange;

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

    @Override
    public Stream<? extends MetaScalar> streamScalars();
    
    @Override
    public abstract Stream<? extends MetaIdentifiedDocPartIndex> streamIndexes();

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

    /**
     *
     * @return
     * @throws IllegalArgumentException if this table already contains a scalar with the
     *                                          same type or name
     */
    public abstract ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws IllegalArgumentException;

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaField> getAddedMetaFields();
    
    public abstract ImmutableMetaField getAddedFieldByIdentifier(String identifier);

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars();

    /**
     * Add a non existent index to this doc part
     * 
     * @param unique
     * @return
     */
    public abstract MutableMetaDocPartIndex addMetaDocPartIndex(boolean unique);

    /**
     * Remove an index from this doc part
     * @param indexId
     * @return
     */
    public boolean removeMetaDocPartIndexByIdentifier(String indexId);
    
    @DoNotChange
    public Iterable<Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> getModifiedMetaDocPartIndexes();
    
    @DoNotChange
    public Iterable<MutableMetaDocPartIndex> getAddedMutableMetaDocPartIndexes();

    public MutableMetaDocPartIndex getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(MetaIndex missingIndex, List<String> identifiers, MetaField newField);
    
    public abstract ImmutableMetaDocPart immutableCopy();
}
