
package com.torodb.torod;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.metainf.*;
import com.torodb.kvdocument.values.KVValue;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public abstract class TorodTransaction implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(TorodTransaction.class);
    private boolean closed = false;
    private final TorodConnection connection;
    
    public TorodTransaction(TorodConnection connection) {
        this.connection = connection;
    }

    public boolean isClosed() {
        return closed;
    }

    final public TorodConnection getConnection() {
        return connection;
    }

    protected abstract InternalTransaction getInternalTransaction();

    public boolean existsCollection(String dbName, String colName) {
        MetaDatabase metaDb = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        return metaDb != null && metaDb.getMetaCollectionByName(colName) != null;
    }
    
    public List<String> getDatabases() {
        return getInternalTransaction().getMetaSnapshot().streamMetaDatabases()
                .map(metaDb -> metaDb.getName()).collect(Collectors.toList());
    }

    public long getDatabaseSize(String dbName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0l;
        }
        return getInternalTransaction().getBackendTransaction().getDatabaseSize(db);
    }
    
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

    public Cursor<ToroDocument> findAll(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyCursor<>();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyCursor<>();
        }
        return getInternalTransaction().getBackendTransaction().findAll(db, col);
    }

    public Cursor<ToroDocument> findByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
            return new EmptyCursor<>();
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            LOGGER.trace("Collection " + dbName + '.' + colName + " does not exist. An empty cursor is returned");
            return new EmptyCursor<>();
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
            return new EmptyCursor<>();
        }

        MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
        if (field == null) {
            LOGGER.trace("Field " + dbName + '.' + colName + '.' + ref + '.' + lastKey + " does not exist. An empty cursor is returned");
            return new EmptyCursor<>();
        }

        return getInternalTransaction().getBackendTransaction().findByField(db, col, docPart, field, value);
    }

    public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return Stream.empty();
        }
        
        return db.streamMetaCollections()
                .map(metaCol -> new CollectionInfo(metaCol.getName(), Json.createObjectBuilder().build()));
    }

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
    public void close() {
        if (!closed) {
            closed = true;
            getInternalTransaction().close();
            connection.onTransactionClosed(this);
        }
    }

}
