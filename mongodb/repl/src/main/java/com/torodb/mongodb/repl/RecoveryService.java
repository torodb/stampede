
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.OplogOperationUnsupported;
import com.eightkdata.mongowp.exceptions.OplogStartMissingException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropDatabaseCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply.DatabaseEntry;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.oplog.OplogOperation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.inject.assistedinject.Assisted;
import com.torodb.common.util.ThreadFactoryRunnableService;
import com.torodb.core.annotations.ToroDbRunnableService;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.repl.OplogManager.OplogManagerPersistException;
import com.torodb.mongodb.repl.OplogManager.WriteTransaction;
import com.torodb.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.mongodb.repl.guice.MongoDbRepl;
import com.torodb.mongodb.utils.DbCloner;
import com.torodb.mongodb.utils.DbCloner.CloneOptions;
import com.torodb.mongodb.utils.DbCloner.CloningException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class RecoveryService extends ThreadFactoryRunnableService {
    private static final int MAX_ATTEMPTS = 10;
    private static final Logger LOGGER = LogManager.getLogger(RecoveryService.class);
    private final Callback callback;
    private final OplogManager oplogManager;
    private final SyncSourceProvider syncSourceProvider;
    private final OplogReaderProvider oplogReaderProvider;
    private final DbCloner cloner;
    private final MongoClientProvider remoteClientProvider;
    private final MongodServer server;
    private final OplogOperationApplier oplogOpApplier;

    @Inject
    public RecoveryService(
            @ToroDbRunnableService ThreadFactory threadFactory,
            @Assisted Callback callback,
            OplogManager oplogManager,
            SyncSourceProvider syncSourceProvider,
            OplogReaderProvider oplogReaderProvider,
            @MongoDbRepl DbCloner cloner,
            MongoClientProvider remoteClientProvider,
            MongodServer server,
            OplogOperationApplier oplogOpApplier) {
        super(threadFactory);
        this.callback = callback;
        this.oplogManager = oplogManager;
        this.syncSourceProvider = syncSourceProvider;
        this.oplogReaderProvider = oplogReaderProvider;
        this.cloner = cloner;
        this.remoteClientProvider = remoteClientProvider;
        this.server = server;
        this.oplogOpApplier = oplogOpApplier;
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

            while (!finished && attempt < MAX_ATTEMPTS && isRunning()) {
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

            MongoConnection remoteConnection = remoteClient.openConnection();

            try (WriteTransaction oplogTransaction = oplogManager.createWriteTransaction()) {
                OplogReader reader = oplogReaderProvider.newReader(remoteConnection);

                OplogOperation lastClonedOp = reader.getLastOp();
                OpTime lastRemoteOptime1 = lastClonedOp.getOpTime();

                LOGGER.info("Remote database cloning started");
                oplogTransaction.truncate();
                LOGGER.info("Local databases dropping started");
                Status<?> status = dropDatabases();
                if (!status.isOK()) {
                    throw new TryAgainException("Error while trying to drop collections: " + status);
                }
                LOGGER.info("Local databases dropping finished");
                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }
                LOGGER.info("Remote database cloning started");
                cloneDatabases(remoteClient);
                LOGGER.info("Remote database cloning finished");

                oplogTransaction.forceNewValue(lastClonedOp.getHash(), lastClonedOp.getOpTime());

                if (!isRunning()) {
                    LOGGER.warn("Recovery stopped before it can finish");
                    return false;
                }

                try (MongodConnection connection = server.openConnection();
                        WriteMongodTransaction trans = connection.openWriteTransaction()) {
                    OpTime lastRemoteOptime2 = reader.getLastOp().getOpTime();
                    LOGGER.info("First oplog application started");
                    applyOplog(trans, oplogTransaction, reader, lastRemoteOptime1, lastRemoteOptime2);
                    trans.commit();
                    LOGGER.info("First oplog application finished");

                    if (!isRunning()) {
                        LOGGER.warn("Recovery stopped before it can finish");
                        return false;
                    }

                    OplogOperation lastOperation = reader.getLastOp();
                    OpTime lastRemoteOptime3 = lastOperation.getOpTime();
                    LOGGER.info("Second oplog application started");
                    applyOplog(trans, oplogTransaction, reader, lastRemoteOptime2, lastRemoteOptime3);
                    trans.commit();
                    LOGGER.info("Second oplog application finished");

                    if (!isRunning()) {
                        LOGGER.warn("Recovery stopped before it can finish");
                        return false;
                    }

                    LOGGER.info("Index rebuild started");
                    rebuildIndexes();
                    trans.commit();
                    LOGGER.info("Index rebuild finished");
                    if (!isRunning()) {
                        LOGGER.warn("Recovery stopped before it can finish");
                        return false;
                    }

                    trans.commit();
                }
            } catch (OplogStartMissingException ex) {
                throw new TryAgainException(ex);
            } catch (OplogOperationUnsupported ex) {
                throw new TryAgainException(ex);
            } catch (MongoException | RollbackException ex) {
                throw new TryAgainException(ex);
            } catch (OplogManagerPersistException ex) {
                throw new FatalErrorException();
            } catch (UserException ex) {
                throw new FatalErrorException(ex);
            }

            callback.setConsistentState(true);

            LOGGER.info("Initial sync finished");
        } finally {
            remoteClient.close();
        }
        return true;
    }

    private void disableInternalIndexes() throws UserException {
        LOGGER.debug("Disabling internal indexes");
        server.getTorodServer().disableInternalIndexes();
        LOGGER.trace("Internal indexes disabled");
    }

    private void enableInternalIndexes() throws UserException {
        LOGGER.debug("Reactivating internal indexes");
        server.getTorodServer().enableInternalIndexes();
        LOGGER.trace("Internal indexes enabled");
    }

    @Override
    protected void shutDown() {
        LOGGER.info("Recived a request to stop the recovering service");
    }

    private Status<?> dropDatabases() throws RollbackException, UserException, RollbackException {

        try (MongodConnection conn = server.openConnection();
                WriteMongodTransaction trans = conn.openWriteTransaction()) {
            Status<ListDatabasesReply> listStatus = trans.execute(
                    new Request("admin", null, true, null),
                    ListDatabasesCommand.INSTANCE,
                    Empty.getInstance()
            );
            if (!listStatus.isOK()) {
                return listStatus;
            }
            assert listStatus.getResult() != null;
            List<DatabaseEntry> dbs = listStatus.getResult().getDatabases();
            for (DatabaseEntry database : dbs) {
                String databaseName = database.getName();
                if (!databaseName.equals("local")) {
                    Status<?> status = trans.execute(
                            new Request(database.getName(), null, true, null),
                            DropDatabaseCommand.INSTANCE,
                            Empty.getInstance()
                    );
                    if (!status.isOK()) {
                        return status;
                    }
                }
            }
            
            trans.commit();
        }
        return Status.ok();
    }

    private void cloneDatabases(@Nonnull MongoClient remoteClient) throws CloningException, MongoException, UserException {
    
        disableInternalIndexes();
        try {
            Stream<String> dbNames;
            try (MongoConnection remoteConnection = remoteClient.openConnection()) {
                dbNames = remoteConnection.execute(
                        ListDatabasesCommand.INSTANCE,
                        "admin",
                        true,
                        Empty.getInstance()
                ).getDatabases().stream().map(db -> db.getName());
            }

            dbNames.filter(this::isReplicable)
                    .forEach(databaseName -> {
                        MyWritePermissionSupplier writePermissionSupplier
                                = new MyWritePermissionSupplier(databaseName);

                        CloneOptions options = new CloneOptions(
                                true,
                                false,
                                true,
                                false,
                                databaseName,
                                Collections.<String>emptySet(),
                                writePermissionSupplier);

                        try {
                            cloner.cloneDatabase(databaseName, remoteClient, server, options);
                        } catch (MongoException ex) {
                            throw new CloningException(ex);
                        }
                    });
        } finally {
            enableInternalIndexes();
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
            WriteMongodTransaction trans,
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
            oplogOpApplier.apply(nextOp, trans, myOplog, false);
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

    private boolean isReplicable(String databaseName) {
        return !databaseName.equals("local");
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

    public static interface RecoveryServiceFactory {
        RecoveryService createRecoveryService(Callback callback);
    }

}
