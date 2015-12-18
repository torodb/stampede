
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropDatabaseCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply.DatabaseEntry;
import com.eightkdata.mongowp.mongoserver.api.safe.oplog.OplogOperation;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.Empty;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.OplogStartMissingException;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.torodb.torod.mongodb.impl.LocalMongoClient;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import com.torodb.torod.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.torod.mongodb.repl.OplogManager.WriteTransaction;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.torod.mongodb.utils.DBCloner;
import com.torodb.torod.mongodb.utils.DBCloner.CloneOptions;
import com.torodb.torod.mongodb.utils.DBCloner.CloningException;
import com.torodb.torod.mongodb.utils.MongoClientProvider;
import com.torodb.torod.mongodb.utils.OplogOperationApplier;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
class RecoveryService extends AbstractExecutionThreadService {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(RecoveryService.class);
    private static final int MAX_ATTEMPTS = 10;
    private final Callback callback;
    private final OplogManager oplogManager;
    private final SyncSourceProvider syncSourceProvider;
    private final OplogReaderProvider oplogReaderProvider;
    private final DBCloner cloner;
    private final MongoClientProvider remoteClientProvider;
    private final LocalMongoClient localClient;
    private final Executor executor;
    private final OplogOperationApplier oplogOpApplier;

    public RecoveryService(
            Callback callback,
            OplogManager oplogManager,
            SyncSourceProvider syncSourceProvider,
            OplogReaderProvider oplogReaderProvider,
            DBCloner cloner,
            MongoClientProvider remoteClientProvider,
            LocalMongoClient localClient,
            OplogOperationApplier oplogOpApplier,
            Executor executor) {
        this.callback = callback;
        this.oplogManager = oplogManager;
        this.syncSourceProvider = syncSourceProvider;
        this.oplogReaderProvider = oplogReaderProvider;
        this.cloner = cloner;
        this.remoteClientProvider = remoteClientProvider;
        this.localClient = localClient;
        this.executor = executor;
        this.oplogOpApplier = oplogOpApplier;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() {
        LOGGER.info("Starting RECOVERY service");
    }

    @Override
    protected void run() throws Exception {
        try {
            int attempt = 0;
            boolean finished = false;

            while (!finished && attempt < MAX_ATTEMPTS) {
                attempt++;
                try {
                    finished = initialSync();
                } catch (TryAgainException ex) {
                    LOGGER.warn("Error while trying to recover (attempt: " + attempt +")", ex);
                } catch (FatalErrorException ex) {
                    LOGGER.error("Fatal error while trying to recover", ex);
                }
            }

            if (!finished) {
                callback.recoveryFailed();
            }
            else {
                callback.recoveryFinished();
            }
        } catch (Throwable ex) {
            callback.recoveryFailed(ex);
        }
    }

    private boolean initialSync() throws TryAgainException, FatalErrorException {
        /*
         * 1.  store that data is inconsistent
         * 2.  decide a sync source
         * 3.  lastRemoteOptime1 = get the last optime of the sync source
         * 4.  clone all databases except local
         * 5.  lastRemoteOptime2 = get the last optime of the sync source
         * 6.  apply remote oplog from lastRemoteOptime1 to lastRemoteOptime2
         * 7.  lastRemoteOptime3 = get the last optime of the sync source
         * 8.  apply remote oplog from lastRemoteOptime2 to lastRemoteOptime3
         * 9.  rebuild indexes
         * 10. store lastRemoteOptime3 as the last applied operation optime
         * 11. store that data is consistent
         * 12. change replication state to SECONDARY
         */

        //TODO: Support fastsync (used to restore a node by copying the data from other up-to-date node)
        LOGGER.info("Starting initial sync");

        callback.setConsistentState(false);

        HostAndPort syncSource;
        try {
            syncSource = syncSourceProvider.calculateSyncSource(null);
            LOGGER.info("Using node " + syncSource + " to replicate from");
        } catch (NoSyncSourceFoundException ex) {
            throw new TryAgainException();
        }

        MongoClient remoteClient;
        try {
            remoteClient = remoteClientProvider.getClient(syncSource);
        } catch (UnreachableMongoServerException ex) {
            throw new TryAgainException(ex);
        }
        try {
            LOGGER.debug("Remote client obtained");

            LocalMongoConnection localConnection = localClient.openConnection();

            MongoConnection remoteConnection = remoteClient.openConnection();

            WriteTransaction oplogTransaction = oplogManager.createWriteTransaction();

            try {
                OplogReader reader = oplogReaderProvider.newReader(remoteConnection);

                OplogOperation lastClonedOp = reader.getLastOp();
                OpTime lastRemoteOptime1 = lastClonedOp.getOpTime();

                LOGGER.info("Remote database cloning started");
                oplogTransaction.truncate();
                LOGGER.info("Local databases dropping started");
                dropDatabases(localConnection);
                LOGGER.info("Local databases dropping finished");
                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }
                LOGGER.info("Remote database cloning started");
                cloneDatabases(remoteConnection, localConnection);
                LOGGER.info("Remote database cloning finished");

                oplogTransaction.forceNewValue(lastClonedOp.getHash(), lastClonedOp.getOpTime());

                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }


                OpTime lastRemoteOptime2 = reader.getLastOp().getOpTime();
                LOGGER.info("First oplog application started");
                applyOplog(localConnection, oplogTransaction, reader, lastRemoteOptime1, lastRemoteOptime2);
                LOGGER.info("First oplog application finished");
                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }

                OplogOperation lastOperation = reader.getLastOp();
                OpTime lastRemoteOptime3 = lastOperation.getOpTime();
                LOGGER.info("Second oplog application started");
                applyOplog(localConnection, oplogTransaction, reader, lastRemoteOptime2, lastRemoteOptime3);
                LOGGER.info("Second oplog application finished");
                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }

                LOGGER.info("Index rebuild started");
                rebuildIndexes();
                LOGGER.info("Index rebuild finished");
                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }
            } catch (OplogStartMissingException ex) {
                throw new TryAgainException(ex);
            } catch (OplogOperationUnsupported ex) {
                throw new TryAgainException(ex);
            } catch (MongoException ex) {
                throw new TryAgainException(ex);
            } catch (CloningException ex) {
                throw new TryAgainException(ex);
            } catch (OplogManagerPersistException ex) {
                throw new FatalErrorException();
            } finally {
                oplogTransaction.close();
            }

            callback.setConsistentState(true);

            LOGGER.info("Initial sync finished");
        } finally {
            remoteClient.close();
        }
        return true;
    }

    @Override
    protected void shutDown() {
        LOGGER.info("Recived a request to stop the recovering service");
    }

    private void dropDatabases(LocalMongoConnection connection) throws MongoException {
        ListDatabasesReply reply = connection.execute(
                ListDatabasesCommand.INSTANCE,
                "admin",
                true,
                Empty.getInstance()
        );
        for (DatabaseEntry database : reply.getDatabases()) {
            String databaseName = database.getName();
            if (!databaseName.equals("local")) {
                connection.execute(
                        DropDatabaseCommand.INSTANCE,
                        database.getName(),
                        true,
                        Empty.getInstance()
                );
            }
        }
    }

    private void cloneDatabases(
            @Nonnull MongoConnection remoteConnection,
            @Nonnull LocalMongoConnection localConnection) throws CloningException, MongoException {

        ListDatabasesReply databasesReply = remoteConnection.execute(
                ListDatabasesCommand.INSTANCE,
                "admin",
                true,
                Empty.getInstance()
        );
        for (DatabaseEntry database : databasesReply.getDatabases()) {
            String databaseName = database.getName();
            MyWritePermissionSupplier writePermissionSupplier = new MyWritePermissionSupplier(databaseName);

            CloneOptions options = new CloneOptions(
                    true,
                    false,
                    true,
                    false,
                    databaseName,
                    Collections.<String>emptySet(),
                    writePermissionSupplier);

            cloner.cloneDatabase(
                    databaseName,
                    remoteConnection,
                    localConnection,
                    options
            );
        }
    }

    /**
     * Applies all the oplog operations stored on the remote server whose
     * optime is higher than <em>from</em> but lower or equal than <em>to</em>.
     * 
     * @param myOplog
     * @param remoteOplog
     * @param to
     * @param from
     */
    private void applyOplog(
            LocalMongoConnection localConnection,
            WriteTransaction myOplog,
            OplogReader remoteOplog,
            OpTime from,
            OpTime to) throws TryAgainException, MongoException, OplogManagerPersistException {

        Iterator<OplogOperation> it = remoteOplog.between(from, true, to, true).iterator();

        if (!it.hasNext()) {
            throw new OplogStartMissingException(remoteOplog.getSyncSource());
        }
        OplogOperation firstOp = it.next();
        if (!firstOp.getOpTime().equals(from)) {
            throw new TryAgainException("Remote oplog does not cointain our last operation");
        }

        OplogOperation nextOp = null;
        while (it.hasNext()) {
            nextOp = it.next();
            if (nextOp.getOpTime().compareTo(to) > 0) {
                throw new TryAgainException("Max optime expected was "+ to + " but an "
                        + "operation whose optime is "+ nextOp.getOpTime() + " "
                        + "was found");
            }
            oplogOpApplier.apply(nextOp, localConnection, myOplog, false);
        }
        if (nextOp != null && !nextOp.getOpTime().equals(to)) {
            LOGGER.warn("Unexpected optime for last operation to apply. "
                    + "Expected " + to + ", but " + nextOp.getOpTime()
                    + " found");
        }
    }
    
    private void rebuildIndexes() {
        //TODO: Check if this is necessary
        LOGGER.warn("Rebuild index is not implemented yet, so indexes have not been rebuild");
    }

    private class MyWritePermissionSupplier implements Supplier<Boolean> {
        private final String database;

        public MyWritePermissionSupplier(String database) {
            this.database = database;
        }
        @Override
        public Boolean get() {
            return callback.canAcceptWrites(database);
        }
    }

    private static class TryAgainException extends Exception {
        private static final long serialVersionUID = 1L;

        public TryAgainException() {
        }

        public TryAgainException(String message) {
            super(message);
        }

        public TryAgainException(String message, Throwable cause) {
            super(message, cause);
        }

        public TryAgainException(Throwable cause) {
            super(cause);
        }
    }

    private static class FatalErrorException extends Exception {
        private static final long serialVersionUID = 1L;

        public FatalErrorException() {
        }

        public FatalErrorException(Throwable cause) {
            super(cause);
        }
    }

    static interface Callback {
        void recoveryFinished();

        void recoveryFailed();

        void recoveryFailed(Throwable ex);

        public void setConsistentState(boolean consistent);

        public boolean canAcceptWrites(String database);
    }

}
