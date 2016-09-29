
package com.torodb.torod.impl.sql;

import java.util.Collection;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.torodb.core.TableRef;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.WriteInternalTransaction;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.torod.SharedWriteTorodTransaction;
import com.torodb.torod.pipeline.InsertPipeline;

/**
 *
 */
public abstract class SqlWriteTorodTransaction<T extends WriteInternalTransaction<?>> extends SqlTorodTransaction<T> implements SharedWriteTorodTransaction {
    
    private final boolean concurrent;
    
    public SqlWriteTorodTransaction(SqlTorodConnection connection, boolean concurrent) {
        super(connection);
        
        this.concurrent = concurrent;
    }

    @Override
    public void insert(String db, String collection, Stream<KVDocument> documents) throws RollbackException, UserException {
        Preconditions.checkState(!isClosed());
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        MutableMetaCollection metaCol = getOrCreateMetaCollection(metaDb, collection);

        //TODO: here we can not use a pipeline
        InsertPipeline pipeline = getConnection().getServer()
                .getInsertPipelineFactory(concurrent)
                .createInsertPipeline(
                        getConnection().getServer().getD2RTranslatorrFactory(),
                        metaDb,
                        metaCol,
                        getInternalTransaction().getBackendTransaction()
                );
        pipeline.insert(documents);
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

        getInternalTransaction().getBackendTransaction().deleteDids(db, col, cursor.getRemaining());
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

        Collection<Integer> dids = getInternalTransaction().getBackendTransaction()
                .findAll(db, col)
                .asDidCursor()
                .getRemaining();
        getInternalTransaction().getBackendTransaction().deleteDids(db, col, dids);
        
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
        
        TableRef tableRef = extractTableRef(attRef);
        String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
        
        MetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
        if (docPart == null) {
            return 0;
        }

        MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
        if (field == null) {
            return 0;
        }

        Collection<Integer> dids = getInternalTransaction().getBackendTransaction()
                .findByField(db, col, docPart, field, value)
                .asDidCursor()
                .getRemaining();
        getInternalTransaction().getBackendTransaction().deleteDids(db, col, dids);
        
        return dids.size();
    }

    @Override
    public void dropCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        MutableMetaCollection metaColl = getMetaCollectionOrThrowException(metaDb, collection);
        
        getInternalTransaction().getBackendTransaction().dropCollection(metaDb, metaColl);

        metaDb.removeMetaCollectionByName(collection);
    }

    @Override
    public void createCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        getOrCreateMetaCollection(metaDb, collection);
    }

    @Override
    public void dropDatabase(String db) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        
        getInternalTransaction().getBackendTransaction().dropDatabase(metaDb);

        getInternalTransaction().getMetaSnapshot().removeMetaDatabaseByName(db);
    }

    @Override
    public boolean createIndex(String dbName, String colName, String indexName, AttributeReference attRef, FieldIndexOrdering ordering, boolean unique) {
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(dbName);
        MutableMetaCollection metaColl = getOrCreateMetaCollection(metaDb, colName);
        
        TableRef tableRef = extractTableRef(attRef);
        String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
        
        boolean indexExists = metaColl.streamContainedMetaIndexes()
                .anyMatch(index -> index.getName().equals(indexName) || (
                        index.isUnique() == unique &&
                        index.getMetaIndexFieldByTableRefAndName(tableRef, lastKey) != null &&
                        index.getMetaIndexFieldByTableRefAndName(tableRef, lastKey).getOrdering() == ordering));
        
        if (!indexExists) {
            MutableMetaIndex metaIndex = metaColl.addMetaIndex(indexName, unique);
            metaIndex.addMetaIndexField(tableRef, lastKey, ordering);
            
            getInternalTransaction().getBackendTransaction().createIndex(metaDb, metaColl, metaIndex);
        }
        
        return !indexExists;
    }

    @Override
    public boolean dropIndex(String dbName, String colName, String indexName) {
        MutableMetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
        if (db == null) {
            return false;
        }
        MutableMetaCollection col = db.getMetaCollectionByName(colName);
        if (col == null) {
            return false;
        }
        MetaIndex index = col.getMetaIndexByName(indexName);
        if (index == null) {
            return false;
        }
        col.removeMetaIndexByName(indexName);

        getInternalTransaction().getBackendTransaction().dropIndex(db, col, index);
        
        return true;
    }

    @Nonnull
    protected MutableMetaDatabase getOrCreateMetaDatabase(String dbName) {
        MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            metaDb = createMetaDatabase(dbName);
        }
        return metaDb;
    }

    private MutableMetaDatabase createMetaDatabase(String dbName) {
        Preconditions.checkState(!isClosed());
        MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.addMetaDatabase(
                dbName,
                getConnection().getServer().getIdentifierFactory().toDatabaseIdentifier(
                        metaSnapshot, dbName)
        );
        getInternalTransaction().getBackendTransaction().addDatabase(metaDb);
        return metaDb;
    }

    protected MutableMetaCollection getOrCreateMetaCollection(@Nonnull MutableMetaDatabase metaDb, String colName) {
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            metaCol = createMetaCollection(metaDb, colName);
        }
        return metaCol;
    }

    protected MutableMetaCollection createMetaCollection(MutableMetaDatabase metaDb, String colName) {
        MutableMetaCollection metaCol;
        Preconditions.checkState(!isClosed());
        metaCol = metaDb.addMetaCollection(
                colName,
                getConnection().getServer().getIdentifierFactory().toCollectionIdentifier(
                        getInternalTransaction().getMetaSnapshot(), metaDb.getName(), colName)
        );
        getInternalTransaction().getBackendTransaction().addCollection(metaDb, metaCol);
        return metaCol;
    }

    @Nonnull
    protected MutableMetaDatabase getMetaDatabaseOrThrowException(@Nonnull String dbName) throws DatabaseNotFoundException {
        MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            throw new DatabaseNotFoundException(dbName);
        }
        return metaDb;
    }

    @Nonnull
    protected MutableMetaCollection getMetaCollectionOrThrowException(@Nonnull MutableMetaDatabase metaDb, @Nonnull String colName) throws CollectionNotFoundException {
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            throw new CollectionNotFoundException(metaDb.getName(), colName);
        }
        return metaCol;
    }

    @Override
    public void commit() throws RollbackException, UserException {
        getInternalTransaction().commit();
    }

}
