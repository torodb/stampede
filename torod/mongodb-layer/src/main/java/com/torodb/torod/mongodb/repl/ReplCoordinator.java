
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.messages.request.QueryMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import com.mongodb.WriteConcern;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.annotations.Locked;
import com.torodb.torod.mongodb.annotations.MongoDBLayer;
import com.torodb.torod.mongodb.impl.LocalMongoClient;
import com.torodb.torod.mongodb.impl.LocalMongoConnection;
import com.torodb.torod.mongodb.repl.exceptions.NoSyncSourceFoundException;
import com.torodb.torod.mongodb.utils.DBCloner;
import com.torodb.torod.mongodb.utils.MongoClientProvider;
import com.torodb.torod.mongodb.utils.OplogOperationApplier;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@ThreadSafe
@Singleton
public class ReplCoordinator extends AbstractIdleService implements ReplInterface {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(ReplCoordinator.class);
    private final ReplCoordinatorOwnerCallback ownerCallback;
    private final ReadWriteLock lock;
    private final OplogReaderProvider oplogReaderProvider;
    private volatile MemberState memberState;
    private final Executor executor;
    private final OplogManager oplogManager;
    private final OplogOperationApplier oplogOpApplier;
    private final LocalMongoClient localClient;
    private final SyncSourceProvider syncSourceProvider;
    private final DBCloner dbCloner;
    private final MongoClientProvider remoteClientProvider;
    private final ObjectId myRID;
    private final int myId;
    private final String defaultDatabase;

    private RecoveryService recoveryService;
    private SecondaryStateService secondaryService;
    private boolean consistent;

    @Inject
    public ReplCoordinator(
            ReplCoordinatorOwnerCallback ownerCallback,
            OplogReaderProvider orpProvider,
            OplogOperationApplier oplogOpApplier,
            LocalMongoClient localClient,
            DBCloner dbCloner,
            MongoClientProvider remoteClientProvider,
            SyncSourceProvider syncSourceProvider,
            OplogManager oplogManager,
            @DatabaseName String defaultDatabase,
            @MongoDBLayer Executor replExecutor) {
        this.ownerCallback = ownerCallback;
        this.executor = replExecutor;
        this.oplogReaderProvider = orpProvider;
        this.oplogManager = oplogManager;
        
        this.lock = new ReentrantReadWriteLock();

        recoveryService = null;
        secondaryService = null;
        memberState = null;

        this.myId = 123;
        this.myRID = new ObjectId();

        this.dbCloner = dbCloner;
        this.remoteClientProvider = remoteClientProvider;
        this.oplogOpApplier = oplogOpApplier;
        this.localClient = localClient;
        this.syncSourceProvider = syncSourceProvider;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting replication service");
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            //TODO: temporal implementation
            loadStoredConfig();
            oplogManager.startAsync();
            oplogManager.awaitRunning();
            loadConsistentState();

            if (iAmMaster()) {
                startPrimaryMode();
            }
            else {
                if (!isConsistent()) {
                    startRecoveryMode();
                }
                else {
                    startSecondaryMode();
                }
            }
        } finally {
            writeLock.unlock();
        }
        LOGGER.info("Replication service started");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Shutting down replication service");
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (recoveryService != null) {
                stopRecoveryMode();
            }
            if (secondaryService != null) {
                stopSecondaryMode();
            }
            memberState = null;
        } finally {
            writeLock.unlock();
        }
        LOGGER.info("Replication service shutted down");
        ownerCallback.replCoordStopped();
    }

    @Override
    public void loadConfiguration(ReplicaSetConfig newConfig) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public MemberStateInterface freezeMemberState(boolean toChangeState) {
        Lock mutex;
        if (toChangeState) {
            mutex = lock.writeLock();
        }
        else {
            mutex = lock.readLock();
        }
        mutex.lock();

        switch (memberState) {
            case RS_PRIMARY: {
                return new MyPrimaryStateInterface(mutex, toChangeState);
            }
            case RS_SECONDARY: {
                return new MySecondaryStateInferface(mutex, toChangeState);
            }
            case RS_RECOVERING: {
                return new MyRecoveryStateInterface(mutex, toChangeState);
            }
            default: {
                throw new AssertionError("State " + memberState + " is not supported yet");
            }
        }
    }

    @Override
    public OplogManager getOplogManager() {
        return oplogManager;
    }

    @Override
    public long getSlaveDelaySecs() {
        return 0;
    }

    @Override
    public ObjectId getRID() {
        return myRID;
    }

    @Override
    public int getId() {
        return myId;
    }

    @Locked(exclusive = true)
    private void startRecoveryMode() {
        if (memberState != null && memberState.equals(MemberState.RS_RECOVERING)) {
            LOGGER.warn("Trying to start RECOVERY mode while we already are in that state");
            assert recoveryService != null;
            return ;
        }
        assert recoveryService == null;
        LOGGER.info("Starting RECOVERY mode");

        memberState = MemberState.RS_RECOVERING;
        recoveryService = new RecoveryService(
                new RecoveryServiceCallback(),
                oplogManager,
                syncSourceProvider,
                oplogReaderProvider,
                dbCloner,
                remoteClientProvider,
                localClient,
                oplogOpApplier,
                executor
        );
        recoveryService.startAsync();
    }

    @Locked(exclusive = true)
    private void stopRecoveryMode() {
        LOGGER.info("Stopping RECOVERY mode");
        recoveryService.stopAsync();
    }

    @Locked(exclusive = true)
    private void startSecondaryMode() {
        if (memberState != null && memberState.equals(MemberState.RS_SECONDARY)) {
            LOGGER.warn("Trying to start SECONDARY mode while we already are in that state");
            assert secondaryService != null;
            return ;
        }
        assert secondaryService == null;

        LOGGER.info("Starting SECONDARY mode");

        memberState = MemberState.RS_SECONDARY;
        secondaryService = new SecondaryStateService(
                new SecondaryServiceCallback(),
                oplogManager,
                oplogReaderProvider,
                oplogOpApplier,
                localClient,
                syncSourceProvider,
                executor
        );

        secondaryService.startAsync();
        try {
            secondaryService.awaitRunning();
        } catch (IllegalStateException ex) {
            LOGGER.error("Fatal error while starting secondary mode", ex);
            this.stopAsync();
        }
    }

    @Locked(exclusive = true)
    private void stopSecondaryMode() {
        Preconditions.checkState(memberState != null && memberState.equals(MemberState.RS_SECONDARY));

        assert secondaryService != null;

        LOGGER.info("Stopping SECONDARY mode");
        secondaryService.stopAsync();
        secondaryService.awaitTerminated();
        secondaryService = null;
    }

    @Locked(exclusive = true)
    private void startRollbackMode() {
        LOGGER.warn("Rollback request ignored. Starting recovery");
        startRecoveryMode();
    }

    @Locked(exclusive = true)
    private void startUnrecoverableMode() {
        LOGGER.info("Starting UNRECOVABLE mode");
        //TODO: Log somewhere
        memberState = null;
    }

    @Locked(exclusive = true)
    private void startPrimaryMode() {
        LOGGER.info("Starting PRIMARY mode");
        memberState = MemberState.RS_PRIMARY;
    }

    private boolean isConsistent() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return consistent;
        } finally {
            readLock.unlock();
        }
    }

    private void setConsistentState(boolean consistent) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            this.consistent = consistent;
            flushConsistentState();
            LOGGER.info("Consistent state set to '" + consistent + "'");
        }
        catch (Throwable ex) {
            LOGGER.error("It was impossible to store the consistent state", ex);
        } finally {
            writeLock.unlock();
        }
    }

    private void flushConsistentState() throws MongoException {
        LocalMongoConnection connection = localClient.openConnection();
        try {
            connection.execute(
                    DeleteCommand.INSTANCE,
                    defaultDatabase,
                    true,
                    new DeleteArgument(
                            "torodb",
                            Collections.singletonList(
                                    new DeleteStatement(
                                        new BsonDocument("consistent",
                                                new BsonDocument("$exists", BsonBoolean.TRUE)),
                                        true
                                    )
                            ),
                            true,
                            WriteConcern.FSYNCED
                    )
            );

            int numInserted = connection.execute(
                    InsertCommand.INSTANCE,
                    defaultDatabase,
                    true,
                    new InsertArgument(
                            "torodb",
                            Collections.singletonList(
                                    new BsonDocument("consistent", BsonBoolean.valueOf(consistent))
                            ),
                            WriteConcern.FSYNCED,
                            true,
                            null
                    )
            ).getN();
            if (numInserted != 1) {
                throw new AssertionError("An invalid number of documents has been inserted: " + numInserted);
            }
        } finally {
            connection.close();
        }
    }

    private void loadConsistentState() throws MongoException {
        LocalMongoConnection connection = localClient.openConnection();
        try {
            EnumSet<QueryMessage.Flag> flags = EnumSet.noneOf(QueryMessage.Flag.class);
            BsonDocument doc = connection.query(
                    defaultDatabase,
                    "torodb",
                    flags,
                    new BsonDocument("consistent", new BsonDocument("$exists", BsonBoolean.TRUE)),
                    0,
                    0,
                    null
            )
                    .getOne();

            boolean newConsistent;
            if (doc == null) {
                newConsistent = false;
            }
            else {
                BsonValue consistentField = doc.get("consistent");
                newConsistent = consistentField != null && consistentField.isBoolean()
                        && consistentField.asBoolean().getValue();
            }
            consistent = newConsistent;
        } finally {
            connection.close();
        }
    }

    private void loadStoredConfig() {
        LOGGER.warn("loadStoredConfig() is not implemented yet");
    }

    private boolean iAmMaster() {
        try {
            syncSourceProvider.calculateSyncSource(null);
        } catch (NoSyncSourceFoundException ex) {
            return true;
        }
        return false;
    }

    public static interface ReplCoordinatorOwnerCallback {
        public void replCoordStopped();
    }

    @NotThreadSafe
    private abstract class MyMemberStateInteface implements MemberStateInterface {
        private final Lock lock;
        private boolean closed;
        private boolean canChangeState;

        public MyMemberStateInteface(Lock lock, boolean canChangeState) {
            this.lock = lock;
            this.closed = false;
            this.canChangeState = true;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                lock.unlock();
            }
        }

        @Override
        public boolean canChangeMemberState() {
            return canChangeState;
        }
    }

    @NotThreadSafe
    private class MyPrimaryStateInterface extends MyMemberStateInteface implements PrimaryStateInterface {

        public MyPrimaryStateInterface(Lock lock, boolean canChangeState) {
            super(lock, canChangeState);
        }

        @Override
        public boolean canNodeAcceptWrites(String database) {
            return true;
        }

        @Override
        public void stepDown(boolean force, long waitTime, long stepDownTime) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public ObjectId getOurElectionId() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public MemberState getMemberState() {
            return MemberState.RS_PRIMARY;
        }

        @Override
        public WriteConcernEnforcementResult awaitReplication(OpTime ts, WriteConcern wc) {
            //TODO: trivial implementation
            return new WriteConcernEnforcementResult(
                    wc,
                    null,
                    0,
                    null,
                    false,
                    null,
                    ImmutableList.<HostAndPort>of()
            );
        }

        @Override
        public boolean canNodeAcceptReads(String database) {
            return true;
        }

    }

    private class MyRecoveryStateInterface extends MyMemberStateInteface implements RecoveryStateInterface {

        public MyRecoveryStateInterface(Lock lock, boolean canChangeState) {
            super(lock, canChangeState);
        }

        @Override
        public MemberState getMemberState() {
            return MemberState.RS_RECOVERING;
        }

        @Override
        public boolean canNodeAcceptWrites(String database) {
//            TODO: Check if the implementation is correct
//            return database.startsWith("local.");
            return false;
        }

        @Override
        public boolean canNodeAcceptReads(String database) {
            return false;
        }

    }

    @ThreadSafe
    private class MySecondaryStateInferface extends MyMemberStateInteface implements SecondaryStateInferface {
        public MySecondaryStateInferface(Lock lock, boolean canChangeState) {
            super(lock, canChangeState);
        }

        @Override
        public MemberState getMemberState() {
            return MemberState.RS_SECONDARY;
        }

        @Override
        public boolean canNodeAcceptWrites(String database) {
            return database.startsWith("local.");
        }

        @Override
        public boolean canNodeAcceptReads(String database) {
            return true;
        }

        @Override
        public void doPause() {
            assert secondaryService != null;
            secondaryService.doPause();
        }

        @Override
        public void doContinue() {
            assert secondaryService != null;
            secondaryService.doContinue();
        }

        @Override
        public boolean isPaused() {
            assert secondaryService != null;
            return secondaryService.isPaused();
        }
    }

    private class RecoveryServiceCallback implements RecoveryService.Callback {

        @Override
        public void recoveryFinished() {
            Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                if (memberState != null && memberState.equals(MemberState.RS_RECOVERING)) {
                    stopRecoveryMode();
                    startSecondaryMode();
                }
                else {
                    LOGGER.info("Recovery finished, but before we can start "
                            + "secondary mode, the state changed to {}", memberState);
                }
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public void recoveryFailed(Throwable ex) {
            LOGGER.error("Fatal error while starting recovery mode", ex);
            stopAsync();
        }

        @Override
        public void recoveryFailed() {
            LOGGER.error("Fatal error while starting recovery mode");
            stopAsync();
        }

        @Override
        public void setConsistentState(boolean consistent) {
            ReplCoordinator.this.setConsistentState(consistent);
        }

        @Override
        public boolean canAcceptWrites(String database) {
            return true;
        }
    }

    private class SecondaryServiceCallback implements SecondaryStateService.Callback {

        @Override
        public void rollbackRequired() {
            Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                if (MemberState.RS_SECONDARY.equals(memberState)) {
                    stopSecondaryMode();
                    startRollbackMode();
                }
                else {
                    LOGGER.info("Secondary request a rollback, but before we "
                            + "can start rollback mode, the state changed to {}",
                            memberState);
                }
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public void impossibleToRecoverFromError(Throwable t) {
            Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                if (MemberState.RS_SECONDARY.equals(memberState)) {
                    stopSecondaryMode();
                    startUnrecoverableMode();
                }
                else {
                    LOGGER.info("Secondary request a rollback, but before we "
                            + "can start rollback mode, the state changed to {}",
                            memberState);
                }
            } finally {
                writeLock.unlock();
            }
        }

    }
}
