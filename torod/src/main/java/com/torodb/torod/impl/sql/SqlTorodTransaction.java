
package com.torodb.torod.impl.sql;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.*;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.metainf.*;
import com.torodb.core.util.AttributeRefKVDocResolver;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.TorodTransaction;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

/**
 *
 */
public abstract class SqlTorodTransaction implements TorodTransaction {

    private static final Logger LOGGER = LogManager.getLogger(SqlTorodTransaction.class);
    private boolean closed = false;
    private final SqlTorodConnection connection;
    
    public SqlTorodTransaction(SqlTorodConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    final public SqlTorodConnection getConnection() {
        return connection;
    }

    protected abstract InternalTransaction getInternalTransaction();

    @Override
    public boolean existsCollection(String dbName, String colName) {
        MetaDatabase metaDb = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        return metaDb != null && metaDb.getMetaCollectionByName(colName) != null;
    }

    @Override
    public List<String> getDatabases() {
        return getInternalTransaction().getMetaSnapshot().streamMetaDatabases()
                .map(metaDb -> metaDb.getName()).collect(Collectors.toList());
    }

    @Override
    public long getDatabaseSize(String dbName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0l;
        }
        return getInternalTransaction().getBackendTransaction().getDatabaseSize(db);
    }

    @Override
    public long countAll(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return 0;
        }
        return getInternalTransaction().getBackendTransaction().countAll(db, col);
    }

    @Override
    public long getCollectionSize(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return 0;
        }
        return getInternalTransaction().getBackendTransaction().getCollectionSize(db, col);
    }

    @Override
    public long getDocumentsSize(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return 0;
        }
        return getInternalTransaction().getBackendTransaction().getDocumentsSize(db, col);
    }

    @Override
    public ToroCursor findAll(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        return getInternalTransaction().getBackendTransaction().findAll(db, col);
    }

    @Override
    public ToroCursor findByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        TableRefFactory tableRefFactory = connection.getServer().getTableRefFactory();
        TableRef ref = tableRefFactory.createRoot();

        if (attRef.getKeys().isEmpty()) {
            throw new IllegalArgumentException("The empty attribute reference is not valid on queries");
        }
        String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
        if (attRef.getKeys().size() > 1) {
            List<Key<?>> keys = attRef.getKeys();
            List<Key<?>> tableKeys = keys.subList(0, keys.size() - 1);
            for (Key<?> key : tableKeys) {
                ref = tableRefFactory.createChild(ref, extractKeyName(key));
            }
        }
        
        MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
        if (docPart == null) {
            LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }

        MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
        if (field == null) {
            LOGGER.trace("Field " + dbName + '.' + colName + '.' + ref + '.' + lastKey + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }

        return getInternalTransaction().getBackendTransaction().findByField(db, col, docPart, field, value);
    }

    @Override
    public ToroCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef, Collection<KVValue<?>> values) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        if (values.isEmpty()) {
            LOGGER.trace("An empty list of values have been given as in condition. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        TableRefFactory tableRefFactory = connection.getServer().getTableRefFactory();
        TableRef ref = tableRefFactory.createRoot();

        if (attRef.getKeys().isEmpty()) {
            throw new IllegalArgumentException("The empty attribute reference is not valid on queries");
        }
        String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
        if (attRef.getKeys().size() > 1) {
            List<Key<?>> keys = attRef.getKeys();
            List<Key<?>> tableKeys = keys.subList(0, keys.size() - 1);
            for (Key<?> key : tableKeys) {
                ref = tableRefFactory.createChild(ref, extractKeyName(key));
            }
        }

        MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
        if (docPart == null) {
            LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }

        Multimap<MetaField, KVValue<?>> valuesMap = ArrayListMultimap.create();
        for (KVValue<?> value : values) {
            MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
            if (field != null) {
                valuesMap.put(field, value);
            }
        }
        return getInternalTransaction().getBackendTransaction().findByFieldIn(db, col, docPart, valuesMap);
    }

    @Override
    public Cursor<Tuple2<Integer, KVValue<?>>> findByAttRefInProjection(String dbName, 
            String colName, AttributeReference attRef, Collection<KVValue<?>> values) {
        //TODO: This implementation is very inneficient! We need to create a low level method that
        //returns the same cursor that is returned by this one
        if (values.isEmpty()) {
            return new EmptyCursor<>();
        }
        LOGGER.warn("A very inefficient implementation of findByAttRefInProjection is being used");
        return new TransformCursor<>(
                findByAttRefIn(dbName, colName, attRef, values)
                        .asDocCursor(),
                toroDoc -> {
                    Optional<KVValue<?>> resolved = AttributeRefKVDocResolver.resolve(attRef, toroDoc.getRoot());
                    assert resolved.isPresent();
                    return new Tuple2<>(toroDoc.getId(), resolved.get());
                }
        );
    }

    @Override
    public ToroCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyToroCursor();
        }
        return getInternalTransaction().getBackendTransaction().fetch(db, col, didCursor);
    }

    @Override
    public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return Stream.empty();
        }
        
        return db.streamMetaCollections()
                .map(metaCol -> new CollectionInfo(metaCol.getName(), Json.createObjectBuilder().build()));
    }

    @Override
    public CollectionInfo getCollectionInfo(String dbName, String colName) throws CollectionNotFoundException {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            throw new CollectionNotFoundException(dbName, colName);
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            throw new CollectionNotFoundException(dbName, colName);
        }
        
        return new CollectionInfo(db.getMetaCollectionByName(colName).getName(), Json.createObjectBuilder().build());
    }

    protected String extractKeyName(Key<?> key) {
        if (key instanceof ObjectKey) {
            return ((ObjectKey) key).getKey();
        }
        else {
            throw new IllegalArgumentException("Keys whose type is not object are not valid on queries");
        }
    }

    @Override
    public void rollback() {
        getInternalTransaction().rollback();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            getInternalTransaction().close();
            connection.onTransactionClosed(this);
        }
    }

}