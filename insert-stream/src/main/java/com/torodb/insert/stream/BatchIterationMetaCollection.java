
package com.torodb.insert.stream;

import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.WrapperMutableMetaCollection;
import com.torodb.core.transaction.metainf.WrapperMutableMetaDocPart;
import java.util.ArrayList;
import java.util.stream.Stream;

/**
 *
 */
public class BatchIterationMetaCollection extends WrapperMutableMetaCollection {

    private final ArrayList<BatchMetaDocPart> changesOnBatch = new ArrayList<>();

    public BatchIterationMetaCollection(ImmutableMetaCollection wrappedCollection, boolean isNew) {
        super(wrappedCollection, (o) -> {});
        streamContainedMetaDocParts()
                .map((docPart) -> (BatchMetaDocPart) docPart)
                .forEach((docPart) -> docPart.newBatch());
    }

    @Override
    protected WrapperMutableMetaDocPart createMetaDocPart(ImmutableMetaDocPart immutable) {
        return new BatchMetaDocPart(immutable, this::onDocPartChange, true);
    }

    @Override
    public final Stream<? extends WrapperMutableMetaDocPart> streamContainedMetaDocParts() {
        return super.streamContainedMetaDocParts();
    }

    public void newBatch() {
        changesOnBatch.stream().forEach((docPart) -> docPart.newBatch());
        changesOnBatch.clear();
    }

    @DoNotChange
    public Iterable<BatchMetaDocPart> getOnBatchModifiedMetaDocParts() {
        return changesOnBatch;
    }

    @Override
    protected void onDocPartChange(WrapperMutableMetaDocPart changedDocPart) {
        super.onDocPartChange(changedDocPart);
        assert changedDocPart instanceof BatchMetaDocPart;

        changesOnBatch.add((BatchMetaDocPart) changedDocPart);
    }

}
