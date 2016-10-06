
package com.torodb.torod.impl.memory;

import com.torodb.core.cursors.*;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.util.AttributeRefKVDocResolver;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodTransaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.lambda.tuple.Tuple2;

/**
 *
 */
public abstract class MemoryTorodTransaction implements TorodTransaction {
    private boolean closed = false;
    private final MemoryTorodConnection connection;

    public MemoryTorodTransaction(MemoryTorodConnection connection) {
        this.connection = connection;
    }

    protected abstract MemoryData.MDTransaction getTransaction();

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public MemoryTorodConnection getConnection() {
        return connection;
    }

    @Override
    public boolean existsCollection(String dbName, String colName) {
        return getTransaction().existsCollection(dbName, colName);
    }

    @Override
    public List<String> getDatabases() {
        return getTransaction().streamDbs().collect(Collectors.toList());
    }

    @Override
    public long countAll(String dbName, String colName) {
        return getTransaction().streamCollection(dbName, colName).count();
    }

    @Override
    public ToroCursor findAll(String dbName, String colName) {
        return createCursor(getTransaction().streamCollection(dbName, colName));
    }

    Stream<ToroDocument> streamByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        return getTransaction().streamCollection(dbName, colName)
                .filter(doc -> {
                    Optional<KVValue<?>> resolved = AttributeRefKVDocResolver.resolve(
                            attRef, doc.getRoot());
                    return resolved.isPresent() && value.equals(resolved.get());
                });
    }

    @Override
    public ToroCursor findByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        return createCursor(streamByAttRef(dbName, colName, attRef, value));
    }

    @Override
    public ToroCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef, Collection<KVValue<?>> values) {
        return createCursor(
                getTransaction().streamCollection(dbName, colName)
                        .filter(doc -> {
                            Optional<KVValue<?>> resolved = AttributeRefKVDocResolver.resolve(
                                    attRef, doc.getRoot());
                            return resolved.isPresent() && values.contains(resolved.get());
                        })
        );
    }

    @Override
    public Cursor<Tuple2<Integer, KVValue<?>>> findByAttRefInProjection(String dbName,
            String colName, AttributeReference attRef, Collection<KVValue<?>> values) {
        Cursor<ToroDocument> docCursor = findByAttRefIn(dbName, colName, attRef, values)
                .asDocCursor();
        return new TransformCursor<>(docCursor, (toroDoc) -> {
            Optional<KVValue<?>> resolved = AttributeRefKVDocResolver.resolve(attRef, toroDoc.getRoot());
            assert resolved.isPresent();
            return new Tuple2<>(toroDoc.getId(), resolved.get());
        });
    }

    @Override
    public ToroCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
        Map<Integer, KVDocument> colData = getTransaction().data.get(dbName, colName);
        return createCursor(didCursor.getRemaining().stream()
                .map(did -> new Tuple2<>(did, colData.get(did)))
                .filter(tuple -> tuple.v2 != null)
                .map(tuple -> new ToroDocument(tuple.v1, tuple.v2))
        );
    }

    private ToroCursor createCursor(Stream<ToroDocument> docsStream) {
        return new DocToroCursor(new IteratorCursor<>(docsStream.iterator()));
    }

    @Override
    public long getDatabaseSize(String dbName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public long getCollectionSize(String dbName, String colName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public long getDocumentsSize(String dbName, String colName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public CollectionInfo getCollectionInfo(String dbName, String colName) throws
            CollectionNotFoundException {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws IndexNotFoundException {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            getTransaction().close();
            connection.onTransactionClosed(this);
        }
    }

}
