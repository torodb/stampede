
package com.torodb.torod;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.exceptions.user.UserException;
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
public class WriteTorodTransaction implements TorodTransaction {

    private boolean closed = false;
    private final TorodConnection connection;
    private final WriteInternalTransaction internalTransaction;
    
    public WriteTorodTransaction(TorodConnection connection) {
        this.connection = connection;
        internalTransaction = connection
                .getServer()
                .getInternalTransactionManager()
                .openWriteTransaction(connection.getBackendConnection());
    }

    public void insert(String db, String collection, Stream<KVDocument> documents) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
        MutableMetaCollection metaCol = getOrCreateMetaCollection(metaDb, collection);

        InsertPipeline pipeline = connection.getServer().getInsertPipelineFactory().createInsertPipeline(
                connection.getServer().getD2RTranslatorrFactory(),
                metaDb,
                metaCol,
                internalTransaction.getBackendConnection()
        );
        pipeline.insert(documents);
    }
    
    public void dropCollection(String db, String collection) throws RollbackException, UserException {
        MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
        MutableMetaCollection metaColl = getMetaCollectionOrThrowException(metaDb, collection);
        
        internalTransaction.getBackendConnection().dropCollection(metaDb, metaColl);
        
        //TODO implement removeCollection in MutableMetaDatabase
        //metaDb.removeCollection(metaColl)
    }

    public void commit() throws RollbackException, UserException {
        internalTransaction.commit();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            internalTransaction.close();
            connection.onTransactionClosed(this);
        }
    }

    @Nonnull
    private MutableMetaDatabase getOrCreateMetaDatabase(String dbName) {
        MutableMetaSnapshot metaSnapshot = internalTransaction.getMetaSnapshot();
        MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

        if (metaDb == null) {
            metaDb = metaSnapshot.addMetaDatabase(
                    dbName,
                    connection.getServer().getIdentifierFactory().toDatabaseIdentifier(metaSnapshot, dbName)
            );
            internalTransaction.getBackendConnection().addDatabase(metaDb);
        }
        return metaDb;
    }

    private MutableMetaCollection getOrCreateMetaCollection(@Nonnull MutableMetaDatabase metaDb, String colName) {
        MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

        if (metaCol == null) {
            metaCol = metaDb.addMetaCollection(
                    colName,
                    connection.getServer().getIdentifierFactory().toCollectionIdentifier(metaDb, colName)
            );
            internalTransaction.getBackendConnection().addCollection(metaDb, metaCol);
        }
        return metaCol;
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

}
