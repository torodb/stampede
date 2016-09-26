
package com.torodb.core.transaction.metainf;

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
    public abstract Stream<? extends MetaDocPartIndex> streamIndexes();

    @Override
    public MetaDocPartIndex getMetaDocPartIndexByIdentifier(String indexId);

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

    @DoNotChange
    public abstract Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars();

    /**
     * Add an index to this doc part
     * @param indexId
     * @param unique
     * @return
     * @throws IllegalArgumentException if this table already contains an index with the
     *                                          same identifier
     */
    public abstract MutableMetaDocPartIndex addMetaDocPartIndex(String indexId, boolean unique) throws IllegalArgumentException;

    /**
     * Remove an index from this doc part
     * @param indexId
     * @return
     */
    public boolean removeMetaDocPartIndexByIdentifier(String indexId);
    
    @DoNotChange
    public Iterable<Tuple2<MutableMetaDocPartIndex, MetaElementState>> getAddedMetaDocPartIndexes();

    public abstract ImmutableMetaDocPart immutableCopy();
}
