
package com.torodb.torod.pipeline;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaScalar;
import com.torodb.core.transaction.metainf.MetaDocPartIndex;
import com.torodb.core.transaction.metainf.MetaScalar;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaDocPartIndex;

/**
 *
 */
public class BatchMetaDocPart implements MutableMetaDocPart {

    private final MutableMetaDocPart delegate;
    private final ArrayList<ImmutableMetaField> fieldsChangesOnBatch = new ArrayList<>();
    private final ArrayList<ImmutableMetaScalar> scalarChangesOnBatch = new ArrayList<>();
    private final Consumer<BatchMetaDocPart> changeConsumer;
    private boolean createdOnCurrentBatch;

    public BatchMetaDocPart(MutableMetaDocPart delegate, Consumer<BatchMetaDocPart> changeConsumer, boolean createdOnCurrentBatch) {
        this.delegate = delegate;
        this.createdOnCurrentBatch = createdOnCurrentBatch;
        this.changeConsumer = changeConsumer;
    }

    public void newBatch() {
        fieldsChangesOnBatch.clear();
        scalarChangesOnBatch.clear();
        createdOnCurrentBatch = false;
    }

    public boolean isCreatedOnCurrentBatch() {
        return createdOnCurrentBatch;
    }

    public void setCreatedOnCurrentBatch(boolean createdOnCurrentBatch) {
        this.createdOnCurrentBatch = createdOnCurrentBatch;
    }

    @DoNotChange
    public Iterable<ImmutableMetaField> getOnBatchModifiedMetaFields() {
        return fieldsChangesOnBatch;
    }

    @DoNotChange
    public Iterable<ImmutableMetaScalar> getOnBatchModifiedMetaScalars() {
        return scalarChangesOnBatch;
    }

    @Override
    public ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws
            IllegalArgumentException {
        ImmutableMetaField newMetaField = delegate.addMetaField(name, identifier, type);

        fieldsChangesOnBatch.add(newMetaField);
        changeConsumer.accept(this);

        return newMetaField;
    }

    @Override
    public ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws
            IllegalArgumentException {
        ImmutableMetaScalar newMetaScalar = delegate.addMetaScalar(identifier, type);

        scalarChangesOnBatch.add(newMetaScalar);
        changeConsumer.accept(this);

        return newMetaScalar;
    }

    @Override
    public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type) {
        return delegate.getMetaFieldByNameAndType(fieldName, type);
    }

    @Override
    public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String fieldName) {
        return delegate.streamMetaFieldByName(fieldName);
    }

    @Override
    public ImmutableMetaField getMetaFieldByIdentifier(String fieldId) {
        return delegate.getMetaFieldByIdentifier(fieldId);
    }

    @Override
    public Stream<? extends ImmutableMetaField> streamFields() {
        return delegate.streamFields();
    }

    @Override
    public Iterable<? extends ImmutableMetaField> getAddedMetaFields() {
        return delegate.getAddedMetaFields();
    }

    @Override
    public ImmutableMetaDocPart immutableCopy() {
        return delegate.immutableCopy();
    }

    @Override
    public TableRef getTableRef() {
        return delegate.getTableRef();
    }

    @Override
    public String getIdentifier() {
        return delegate.getIdentifier();
    }

    @Override
    public Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars() {
        return delegate.getAddedMetaScalars();
    }

    @Override
    public Stream<? extends MetaScalar> streamScalars() {
        return delegate.streamScalars();
    }

    @Override
    public String toString() {
        return defautToString();
    }

    @Override
    public Stream<? extends MetaDocPartIndex> streamIndexes() {
        return delegate.streamIndexes();
    }

    @Override
    public MetaDocPartIndex getMetaDocPartIndexByIdentifier(String indexId) {
        return delegate.getMetaDocPartIndexByIdentifier(indexId);
    }

    @Override
    public MutableMetaDocPartIndex addMetaDocPartIndex(String identifier, boolean unique) throws IllegalArgumentException {
        return delegate.addMetaDocPartIndex(identifier, unique);
    }

    @Override
    public Iterable<? extends MutableMetaDocPartIndex> getAddedMetaDocPartIndexes() {
        return delegate.getAddedMetaDocPartIndexes();
    }
}
