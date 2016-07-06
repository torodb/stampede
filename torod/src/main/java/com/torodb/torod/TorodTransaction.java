
package com.torodb.torod;

import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.metainf.*;
import com.torodb.kvdocument.values.KVValue;
import java.util.List;
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

    public Stream<CollectionInfo> getCollectionsInfo() {
        return getInternalTransaction().getMetaSnapshot().streamMetaDatabases()
                .map(metaCol -> new CollectionInfo(metaCol.getName(), Json.createObjectBuilder().build()));
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
