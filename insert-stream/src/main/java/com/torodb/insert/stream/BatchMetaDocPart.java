
package com.torodb.insert.stream;

import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 *
 */
public class BatchMetaDocPart extends WrapperMutableMetaDocPart {

    private boolean createdOnCurrentBatch;
    private final ArrayList<ImmutableMetaField> changesOnBatch = new ArrayList<>();

    public BatchMetaDocPart(ImmutableMetaDocPart wrapped, Consumer<WrapperMutableMetaDocPart> changeConsumer, boolean createdOnCurrentBatch) {
        super(wrapped, changeConsumer);
        this.createdOnCurrentBatch = createdOnCurrentBatch;
    }

    public void newBatch() {
        changesOnBatch.clear();
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
        return changesOnBatch;
    }

    @Override
    public ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws
            IllegalArgumentException {
        ImmutableMetaField newMetaField = super.addMetaField(name, identifier, type);

        changesOnBatch.add(newMetaField);

        return newMetaField;
    }
}
