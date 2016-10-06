
package com.torodb.torod.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.ImmutableMetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaElementState;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.core.transaction.metainf.MutableMetaIndex;

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
    public MutableMetaIndex getMetaIndexByName(String indexName) {
        return delegate.getMetaIndexByName(indexName);
    }

    @Override
    public Stream<? extends MutableMetaIndex> streamContainedMetaIndexes() {
        return delegate.streamContainedMetaIndexes();
    }

    @Override
    public MutableMetaIndex addMetaIndex(String name, boolean unique) throws IllegalArgumentException {
        return delegate.addMetaIndex(name, unique);
    }

    @Override
    public boolean removeMetaIndexByName(String indexName) {
        return delegate.removeMetaIndexByName(indexName);
    }

    @Override
    public Iterable<Tuple2<MutableMetaIndex, MetaElementState>> getModifiedMetaIndexes() {
        return delegate.getModifiedMetaIndexes();
    }

    @Override
    public List<Tuple2<MetaIndex, List<String>>> getMissingIndexesForNewField(MutableMetaDocPart docPart,
            MetaField newField) {
        return delegate.getMissingIndexesForNewField(docPart, newField);
    }

    @Override
    public Optional<? extends MetaIndex> getAnyMissedIndex(MetaCollection oldCol, MutableMetaDocPart newStructure,
            ImmutableMetaDocPart oldStructure, ImmutableMetaField newField) {
        return delegate.getAnyMissedIndex(oldCol, newStructure, oldStructure, newField);
    }

    @Override
    public Optional<? extends MetaIndex> getAnyRelatedIndex(ImmutableMetaCollection oldCol, MetaDocPart newStructure,
            ImmutableMetaIdentifiedDocPartIndex newDocPartIndex) {
        return delegate.getAnyRelatedIndex(oldCol, newStructure, newDocPartIndex);
    }

    @Override
    public Optional<ImmutableMetaIndex> getAnyMissedIndex(ImmutableMetaCollection oldCol,
            ImmutableMetaIdentifiedDocPartIndex newRemovedDocPartIndex) {
        return delegate.getAnyMissedIndex(oldCol, newRemovedDocPartIndex);
    }

    @Override
    public Optional<ImmutableMetaIndex> getAnyConflictingIndex(ImmutableMetaCollection oldStructure,
            MutableMetaIndex changed) {
        return delegate.getAnyConflictingIndex(oldStructure, changed);
    }

    @Override
    public Optional<ImmutableMetaDocPart> getAnyDocPartWithMissedDocPartIndex(ImmutableMetaCollection oldStructure,
            MutableMetaIndex changed) {
        return delegate.getAnyDocPartWithMissedDocPartIndex(oldStructure, changed);
    }

    @Override
    public Optional<? extends MetaIdentifiedDocPartIndex> getAnyOrphanDocPartIndex(ImmutableMetaCollection oldStructure,
            MutableMetaIndex changed) {
        return delegate.getAnyOrphanDocPartIndex(oldStructure, changed);
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

}
