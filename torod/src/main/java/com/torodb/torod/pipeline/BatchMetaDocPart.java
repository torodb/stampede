
package com.torodb.torod.pipeline;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.*;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.stream.Stream;

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
    public Iterable<? extends MetaScalar> getAddedMetaScalars() {
        return delegate.getAddedMetaScalars();
    }

    @Override
    public Stream<? extends MetaScalar> streamScalars() {
        return delegate.streamScalars();
    }
}
