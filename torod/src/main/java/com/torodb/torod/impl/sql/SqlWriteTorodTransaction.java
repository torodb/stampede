
package com.torodb.torod.impl.sql;

import com.google.common.base.Preconditions;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.WriteInternalTransaction;
import com.torodb.core.transaction.metainf.*;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.WriteTorodTransaction;
import com.torodb.torod.pipeline.InsertPipeline;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 *
 */
public class SqlWriteTorodTransaction extends SqlTorodTransaction implements WriteTorodTransaction {

    private final WriteInternalTransaction internalTransaction;
    private final boolean concurrent;
    
    public SqlWriteTorodTransaction(SqlTorodConnection connection, boolean concurrent) {
        super(connection);
        internalTransaction = connection
                .getServer()
                .getInternalTransactionManager()
                .openWriteTransaction(getConnection().getBackendConnection());
        this.concurrent = concurrent;
    }

    @Override
    public void insert(String db, String collection, Stream<KVDocument> documents) throws RollbackException, UserException {
        Preconditions.checkState(!isClosed());
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        MutableMetaCollection metaCol = getOrCreateMetaCollection(metaDb, collection);

        InsertPipeline pipeline = getConnection().getServer()
                .getInsertPipelineFactory(concurrent)
                .createInsertPipeline(
                        getConnection().getServer().getD2RTranslatorrFactory(),
                        metaDb,
                        metaCol,
                        internalTransaction.getBackendConnection()
                );
        pipeline.insert(documents);
    }

    @Nonnull
    private MutableMetaDatabase getOrCreateMetaDatabase(String dbName) {
        MutableMetaSnapshot metaSnapshot = internalTransaction.getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            metaDb = createMetaDatabase(dbName);
        }
        return metaDb;
    }

    private MutableMetaDatabase createMetaDatabase(String dbName) {
        Preconditions.checkState(!isClosed());
        MutableMetaSnapshot metaSnapshot = internalTransaction.getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.addMetaDatabase(
                dbName,
                getConnection().getServer().getIdentifierFactory().toDatabaseIdentifier(
                        metaSnapshot, dbName)
        );
        internalTransaction.getBackendConnection().addDatabase(metaDb);
        return metaDb;
    }

    private MutableMetaCollection getOrCreateMetaCollection(@Nonnull MutableMetaDatabase metaDb, String colName) {
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            metaCol = createMetaCollection(metaDb, colName);
        }
        return metaCol;
    }

    private MutableMetaCollection createMetaCollection(MutableMetaDatabase metaDb, String colName) {
        MutableMetaCollection metaCol;
        Preconditions.checkState(!isClosed());
        metaCol = metaDb.addMetaCollection(
                colName,
                getConnection().getServer().getIdentifierFactory().toCollectionIdentifier(
                        internalTransaction.getMetaSnapshot(), metaDb.getName(), colName)
        );
        internalTransaction.getBackendConnection().addCollection(metaDb, metaCol);
        return metaCol;
    }

    @Override
    public void delete(String dbName, String colName, Cursor<Integer> cursor) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return;
        }

        internalTransaction.getBackendTransaction().deleteDids(db, col, cursor.getRemaining());
    }
    
    @Override
    public long deleteAll(String dbName, String colName) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return 0;
        }

        Collection<Integer> dids = internalTransaction.getBackendTransaction()
                .findAll(db, col)
                .asDidCursor()
                .getRemaining();
        internalTransaction.getBackendTransaction().deleteDids(db, col, dids);
        
        return dids.size();
    }

    @Override
    public long deleteByAttRef(String dbName, String colName, AttributeReference attRef, KVValue<?> value) {
        MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return 0;
        }
        MetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return 0;
        }
        TableRefFactory tableRefFactory = getConnection().getServer().getTableRefFactory();
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
            return 0;
        }

        MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
        if (field == null) {
            return 0;
        }

        Collection<Integer> dids = internalTransaction.getBackendTransaction()
                .findByField(db, col, docPart, field, value)
                .asDidCursor()
                .getRemaining();
        internalTransaction.getBackendTransaction().deleteDids(db, col, dids);
        
        return dids.size();
    }

    @Override
    public void dropCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        MutableMetaCollection metaColl = getMetaCollectionOrThrowException(metaDb, collection);
        
        internalTransaction.getBackendConnection().dropCollection(metaDb, metaColl);

        metaDb.removeMetaCollectionByName(collection);
    }

    @Override
    public void renameCollection(String fromDb, String fromCollection, String toDb, String toCollection) throws RollbackException, UserException {
        MutableMetaDatabase fromMetaDb = getMetaDatabaseOrThrowException(fromDb);
        MetaCollection fromMetaColl = getMetaCollectionOrThrowException(fromMetaDb, fromCollection);

        MutableMetaDatabase toMetaDb = getOrCreateMetaDatabase(toDb);
        MutableMetaCollection toMetaColl = createMetaCollection(toMetaDb, toCollection);
        
        internalTransaction.getBackendConnection().renameCollection(fromMetaDb, fromMetaColl, toMetaDb, toMetaColl);

        fromMetaDb.removeMetaCollectionByName(fromCollection);
    }

    @Override
    public void createCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        getOrCreateMetaCollection(metaDb, collection);
    }

    @Override
    public void dropDatabase(String db) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        
        internalTransaction.getBackendConnection().dropDatabase(metaDb);

        internalTransaction.getMetaSnapshot().removeMetaDatabaseByName(db);
    }

    @Nonnull
    private MutableMetaDatabase getMetaDatabaseOrThrowException(@Nonnull String dbName) throws DatabaseNotFoundException {
        MutableMetaSnapshot metaSnapshot = internalTransaction.getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            throw new DatabaseNotFoundException(dbName);
        }
        return metaDb;
    }

    @Nonnull
    private MutableMetaCollection getMetaCollectionOrThrowException(@Nonnull MutableMetaDatabase metaDb, @Nonnull String colName) throws CollectionNotFoundException {
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            throw new CollectionNotFoundException(metaDb.getName(), colName);
        }
        return metaCol;
    }

    @Override
    protected InternalTransaction getInternalTransaction() {
        return internalTransaction;
    }

    @Override
    public void commit() throws RollbackException, UserException {
        internalTransaction.commit();
    }

}
