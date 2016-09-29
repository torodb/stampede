
package com.torodb.torod.impl.memory;

import java.util.stream.Stream;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.ExclusiveWriteTorodTransaction;
import com.torodb.torod.impl.memory.MemoryData.MDTransaction;

/**
 *
 */
public class MemoryWriteTorodTransaction extends MemoryTorodTransaction implements ExclusiveWriteTorodTransaction {

    private final MemoryData.MDWriteTransaction trans;

    public MemoryWriteTorodTransaction(MemoryTorodConnection connection) {
        super(connection);
        this.trans = connection.getServer().getData().openWriteTransaction();
    }

    @Override
    protected MDTransaction getTransaction() {
        return trans;
    }

    @Override
    public void insert(String db, String collection, Stream<KVDocument> documents) throws
            RollbackException, UserException {
        trans.insert(db, collection, documents);
    }

    @Override
    public long deleteAll(String dbName, String colName) {
        long count = trans.streamCollection(dbName, colName).count();
        trans.deleteAll(dbName, colName);
        return count;
    }

    @Override
    public long deleteByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        return trans.delete(dbName, colName, streamByAttRef(dbName, colName, attRef, value).map(ToroDocument::getId));
    }

    @Override
    public void delete(String dbName, String colName, Cursor<Integer> cursor) {
        trans.delete(dbName, colName, cursor.getRemaining().stream());
    }

    @Override
    public void dropCollection(String dbName, String colName) throws RollbackException, UserException {
        trans.dropCollection(dbName, colName);
    }

    @Override
    public void renameCollection(String fromDb, String fromCollection, String toDb, String toCollection)
            throws RollbackException, UserException {
        trans.renameCollection(fromDb, fromCollection, toDb, toCollection);
    }

    @Override
    public void createCollection(String dbName, String colName) throws RollbackException,
            UserException {
        trans.createCollection(dbName, colName);
    }

    @Override
    public void dropDatabase(String dbName) throws RollbackException, UserException {
        trans.dropDatabase(dbName);
    }

    @Override
    public boolean createIndex(String dbName, String colName, String indexName, AttributeReference attRef,
            FieldIndexOrdering ordering, boolean unique) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public boolean dropIndex(String dbName, String colName, String indexName) {
        throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
    }

    @Override
    public void rollback() {
        trans.rollback();
    }

    @Override
    public void commit() throws RollbackException, UserException {
        trans.commit();
    }

}
