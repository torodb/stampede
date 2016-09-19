
package com.torodb.torod.pipeline;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaIndex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class BatchMetaCollection implements MutableMetaCollection {

    private final MutableMetaCollection delegate;
    private final Map<TableRef, BatchMetaDocPart> docPartsByRef;
    private final ArrayList<BatchMetaDocPart> changesOnBatch = new ArrayList<>();
    private final HashSet<BatchMetaDocPart> modifiedDocParts = new HashSet<>();

    private static final Logger LOGGER = LogManager.getLogger(BatchMetaCollection.class);

    public BatchMetaCollection(MutableMetaCollection delegate) {
        this.delegate = delegate;

        docPartsByRef = new HashMap<>();

        delegate.streamContainedMetaDocParts()
                .map((docPart) -> new BatchMetaDocPart(docPart, this::onDocPartChange, false))
                .forEach((docPart) -> docPartsByRef.put(docPart.getTableRef(), docPart));
    }

    @Override
    public final Stream<? extends BatchMetaDocPart> streamContainedMetaDocParts() {
        return docPartsByRef.values().stream();
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
    public BatchMetaDocPart getMetaDocPartByTableRef(TableRef tableRef) {
        return docPartsByRef.get(tableRef);
    }

    @Override
    public BatchMetaDocPart getMetaDocPartByIdentifier(String docPartId) {
        LOGGER.debug("Looking for doc parts on a unidexed way");
        return streamContainedMetaDocParts()
                .filter((dp) -> dp.getIdentifier().equals(docPartId))
                .findAny()
                .orElse(null);
    }

    @Override
    public BatchMetaDocPart addMetaDocPart(TableRef tableRef, String identifier) throws
            IllegalArgumentException {
        MutableMetaDocPart delegateDocPart = delegate.addMetaDocPart(tableRef, identifier);

        BatchMetaDocPart myDocPart = new BatchMetaDocPart(delegateDocPart, this::onDocPartChange, true);
        docPartsByRef.put(myDocPart.getTableRef(), myDocPart);

        changesOnBatch.add(myDocPart);
        modifiedDocParts.add(myDocPart);

        return myDocPart;
    }

    @Override
    @DoNotChange
    public Iterable<? extends MutableMetaDocPart> getModifiedMetaDocParts() {
        return modifiedDocParts;
    }

    @Override
    public ImmutableMetaCollection immutableCopy() {
        return delegate.immutableCopy();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public String getIdentifier() {
        return delegate.getIdentifier();
    }

    @Override
    public String toString() {
        return defautToString();
    }

    private void onDocPartChange(BatchMetaDocPart changedDocPart) {
        changesOnBatch.add(changedDocPart);
        modifiedDocParts.add(changedDocPart);
    }

    @Override
    public MetaIndex getMetaIndexByName(String indexName) {
        return delegate.getMetaIndexByName(indexName);
    }

    @Override
    public Stream<? extends MutableMetaIndex> streamContainedMetaIndexes() {
        return delegate.streamContainedMetaIndexes();
    }

    @Override
    public MutableMetaIndex addMetaIndex(String name, boolean unique) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<? extends MutableMetaIndex> getModifiedMetaIndexes() {
        return Collections.emptyList();
    }

}
