
package com.torodb.torod;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.cursors.ToroCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public interface SharedWriteTorodTransaction extends TorodTransaction {

    public void insert(String dbName, String colName, Stream<KVDocument> documents) throws RollbackException, UserException;

    public default void delete(String dbName, String colName, List<ToroDocument> candidates) {
        delete(dbName, colName, new IteratorCursor<>(candidates.stream().map(ToroDocument::getId).iterator()));
    }

    public default void delete(String dbName, String colName, ToroCursor cursor) {
        delete(dbName, colName, cursor.asDidCursor());
    }

    public void delete(String dbName, String colName, Cursor<Integer> cursor);

    public long deleteAll(String dbName, String colName);

    public long deleteByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value);
    
    public void dropCollection(String db, String collection) throws RollbackException, UserException;
    
    public void createCollection(String db, String collection) throws RollbackException, UserException;
    
    public void dropDatabase(String db) throws RollbackException, UserException;

    public void createIndex(String dbName, String colName, String indexName, AttributeReference attRef, FieldIndexOrdering ordering, boolean unique);

    public void dropIndex(String dbName, String colName, String indexName);

    public void commit() throws RollbackException, UserException;

}
