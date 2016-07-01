
package com.torodb.torod;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.WriteInternalTransaction;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.torod.pipeline.InsertPipeline;

/**
 *
 */
public class WriteTorodTransaction extends TorodTransaction {

    private final WriteInternalTransaction internalTransaction;
    
    public WriteTorodTransaction(TorodConnection connection) {
        super(connection);
        internalTransaction = connection
                .getServer()
                .getInternalTransactionManager()
                .openWriteTransaction(getConnection().getBackendConnection());
    }

    public void insert(String db, String collection, Stream<KVDocument> documents) throws RollbackException, UserException {
        Preconditions.checkState(!isClosed());
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        MutableMetaCollection metaCol = getOrCreateMetaCollection(metaDb, collection);

        InsertPipeline pipeline = getConnection().getServer().getInsertPipelineFactory().createInsertPipeline(
                getConnection().getServer().getD2RTranslatorrFactory(),
                metaDb,
                metaCol,
                internalTransaction.getBackendConnection()
        );
        pipeline.insert(documents);
    }

    @Nonnull
    private MutableMetaDatabase getOrCreateMetaDatabase(String dbName) {
        Preconditions.checkState(!isClosed());
        MutableMetaSnapshot metaSnapshot = internalTransaction.getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            metaDb = metaSnapshot.addMetaDatabase(
                    dbName,
                    getConnection().getServer().getIdentifierFactory().toDatabaseIdentifier(metaSnapshot, dbName)
            );
            internalTransaction.getBackendConnection().addDatabase(metaDb);
        }
        return metaDb;
    }

    private MutableMetaCollection getOrCreateMetaCollection(@Nonnull MutableMetaDatabase metaDb, String colName) {
        Preconditions.checkState(!isClosed());
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            metaCol = metaDb.addMetaCollection(
                    colName,
                    getConnection().getServer().getIdentifierFactory().toCollectionIdentifier(metaDb, colName)
            );
            internalTransaction.getBackendConnection().addCollection(metaDb, metaCol);
        }
        return metaCol;
    }
    
    public void dropCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        MutableMetaCollection metaColl = getMetaCollectionOrThrowException(metaDb, collection);
        
        internalTransaction.getBackendConnection().dropCollection(metaDb, metaColl);
        
        //TODO implement removeCollection in MutableMetaDatabase
        //metaDb.removeCollection(metaColl)
    }
    
    public void dropDatabase(String db) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        
        internalTransaction.getBackendConnection().dropDatabase(metaDb);
        
        //TODO implement removeCollection in MutableMetaDatabase
        //metaDb.removeCollection(metaColl)
        //TODO implement removeDatabase in MutableMetaSnapshot
        //internalTransaction.getMetaSnapshot().removeDatabase(metaDb)
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

    public void commit() throws RollbackException, UserException {
        internalTransaction.commit();
    }

}
