
package com.torodb.mongodb.repl;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonObjectId;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.DefaultBsonValues;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.server.api.Request;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.RollbackException;
import com.torodb.mongodb.annotations.Locked;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.mongodb.repl.oplogreplier.OplogApplierService;
import com.torodb.mongodb.repl.oplogreplier.RollbackReplicationException;
import com.torodb.mongodb.repl.topology.TopologyService;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.eightkdata.mongowp.bson.utils.DefaultBsonValues.*;

/**
 *
 */
@ThreadSafe
@Singleton
public class ReplCoordinator extends ThreadFactoryIdleService implements ReplInterface {
    private static final Logger LOGGER = LogManager.getLogger(ReplCoordinator.class);
    private static final String CONSISTENT_DB = "torodb";
    private static final String CONSISTENT_COL = "repl.consistent";
    private final ReplCoordinatorOwnerCallback ownerCallback;
    private final ReadWriteLock lock;
    private volatile MemberState memberState;
    private final OplogManager oplogManager;
    private final MongodServer server;
    private final SyncSourceProvider syncSourceProvider;
    private final BsonObjectId myRID;
    private final int myId;
    private final RecoveryService.RecoveryServiceFactory recoveryServiceFactory;
    private final OplogApplierService.OplogApplierServiceFactory oplogReplierFactory;
    private final ReplMetrics metrics;
    private final Executor executor;
    private final TopologyService topologyService;

    private RecoveryService recoveryService;
    private OplogApplierService oplogReplierService;
    private boolean consistent;

    @Inject
    public ReplCoordinator(
            @ToroDbIdleService ThreadFactory threadFactory,
            ReplCoordinatorOwnerCallback ownerCallback,
            MongodServer server,
            SyncSourceProvider syncSourceProvider,
            OplogManager oplogManager,
            ObjectIdFactory objectIdFactory,
            RecoveryService.RecoveryServiceFactory recoveryServiceFactory,
            OplogApplierService.OplogApplierServiceFactory oplogReplierFactory,
            ReplMetrics metrics, TopologyService topologyService) {
        super(threadFactory);
        this.ownerCallback = ownerCallback;
        this.oplogManager = oplogManager;
        this.recoveryServiceFactory = recoveryServiceFactory;
        this.oplogReplierFactory = oplogReplierFactory;
        this.metrics = metrics;
        
        this.lock = new ReentrantReadWriteLock();

        recoveryService = null;
        oplogReplierService = null;
        memberState = null;

        this.myId = 123;
        this.myRID = objectIdFactory.consumeObjectId();

        this.server = server;
        this.syncSourceProvider = syncSourceProvider;
        final ThreadFactory utilityThreadFactory = new ThreadFactoryBuilder()
                .setThreadFactory(threadFactory)
                .setNameFormat("repl-coord-util-%d")
                .build();
        this.executor = (Runnable command) -> {
            utilityThreadFactory.newThread(command).start();
        };
        this.topologyService = topologyService;
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting replication service");
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            //TODO: temporal implementation
            loadStoredConfig();

            topologyService.startAsync();
            topologyService.awaitRunning();

            oplogManager.startAsync();
            oplogManager.awaitRunning();
            loadConsistentState();

            if (!isConsistent()) {
                startRecoveryMode();
            }
            else {
                startSecondaryMode();
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
                stopRecoveryModeAsync();
            }
            if (oplogReplierService != null) {
                stopSecondaryModeAsync();
            }
            memberState = null;
        } finally {
            writeLock.unlock();
        }
        awaitRecoveryStopped();
        awaitSecondaryStopped();
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

    public void setMemberState(MemberState memberState) {
        this.memberState = memberState;
        
        if (memberState != null) {
            metrics.getMemberState().setValue(memberState.name());
            metrics.getMemberStateCounters().get(memberState).inc();
        } else {
            metrics.getMemberState().setValue(null);
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
    public BsonObjectId getRID() {
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

        setMemberState(MemberState.RS_RECOVERING);
        recoveryService = recoveryServiceFactory.createRecoveryService(new RecoveryServiceCallback());
        recoveryService.startAsync();
    }

    @Locked(exclusive = true)
    private void stopRecoveryModeAsync() {
        LOGGER.info("Stopping RECOVERY mode");
        recoveryService.stopAsync();
    }

    private void awaitRecoveryStopped() {
        if (recoveryService != null) {
            recoveryService.awaitTerminated();
            recoveryService = null;
        }
    }

    @Locked(exclusive = true)
    private void startSecondaryMode() {
        if (memberState != null && memberState.equals(MemberState.RS_SECONDARY)) {
            LOGGER.warn("Trying to start SECONDARY mode while we already are in that state");
            assert oplogReplierService != null;
            return ;
        }
        assert oplogReplierService == null;

        LOGGER.info("Starting SECONDARY mode");

        setMemberState(MemberState.RS_SECONDARY);
        oplogReplierService = oplogReplierFactory.createOplogApplier(new OplogReplierCallback());

        oplogReplierService.startAsync();
        try {
            oplogReplierService.awaitRunning();
        } catch (IllegalStateException ex) {
            LOGGER.error("Fatal error while starting secondary mode", ex);
            this.stopAsync();
        }
    }

    @Locked(exclusive = true)
    private void stopSecondaryModeAsync() {
        LOGGER.info("Stopping SECONDARY mode");
        oplogReplierService.stopAsync();
    }

    private void awaitSecondaryStopped() {
        if (oplogReplierService != null) {
            try {
                oplogReplierService.awaitTerminated();
                oplogReplierService = null;
            } catch (IllegalStateException ex) {
                LOGGER.error("Error while stopping secondary mode", ex);
                startUnrecoverableMode();
                throw ex;
            }
        }
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
        setMemberState(null);
    }

    @Locked(exclusive = true)
    private void startPrimaryMode() {
        LOGGER.info("Starting PRIMARY mode");
        setMemberState(MemberState.RS_PRIMARY);
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

    private Status<?> setConsistentState(boolean consistent) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            while (true) {
                try {
                    this.consistent = consistent;
                    Status<?> result = flushConsistentState();
                    LOGGER.info("Consistent state set to '" + consistent + "'");

                    return result;
                } catch (RollbackException ex) {
                    LOGGER.warn("Rollback while trying to set the consistent state", ex);
                }
            }
        }
        catch (Throwable ex) {
            LOGGER.error("It was impossible to store the consistent state", ex);
            return Status.from(ErrorCode.UNKNOWN_ERROR, "It was impossible to store the consistent state " + ex);
        } finally {
            writeLock.unlock();
        }
    }

    private Status<?> flushConsistentState() throws RollbackException {
        try (MongodConnection conn = server.openConnection();
                WriteMongodTransaction trans = conn.openWriteTransaction()) {
            Status<Long> deleteStatus = trans.execute(
                    new Request(CONSISTENT_DB, null, true, null),
                    DeleteCommand.INSTANCE,
                    new DeleteArgument(
                            CONSISTENT_COL,
                            Collections.singletonList(
                                    new DeleteStatement(
                                            DefaultBsonValues.EMPTY_DOC,
                                            true
                                    )
                            ),
                            true,
                            WriteConcern.fsync()
                    )
            );
            if (!deleteStatus.isOk()) {
                return deleteStatus;
            }

            Status<InsertResult> insertStatus = trans.execute(
                    new Request(CONSISTENT_DB, null, true, null),
                    InsertCommand.INSTANCE,
                    new InsertArgument(
                            CONSISTENT_COL,
                            Collections.singletonList(
                                    newDocument("consistent", newBoolean(consistent))
                            ),
                            WriteConcern.fsync(),
                            true,
                            null
                    )
            );
            if (!insertStatus.isOk()) {
                return insertStatus;
            }
            if (insertStatus.getResult().getN() != 1) {
                return Status.from(ErrorCode.UNKNOWN_ERROR, "An invalid number of documents has "
                        + "been inserted: " + insertStatus.getResult().getN());
            }
            trans.commit();
            return Status.ok();
        } catch (UserException ex) {
            return Status.from(ErrorCode.UNKNOWN_ERROR, "Unexpected user exception: " + ex);
        }
    }

    private void loadConsistentState() throws MongoException {
        try (MongodConnection conn = server.openConnection();
                ReadOnlyMongodTransaction trans = conn.openReadOnlyTransaction()) {
            Status<FindResult> findStatus = trans.execute(
                    new Request(CONSISTENT_DB, null, true, null),
                    FindCommand.INSTANCE,
                    new FindArgument.Builder()
                    .setCollection(CONSISTENT_COL)
                    .build()
            );
            if (!findStatus.isOk()) {
                throw new UnknownErrorException(findStatus.getErrorMsg());
            }
            boolean newConsistent;
            Iterator<BsonDocument> firstBatch = findStatus.getResult().getCursor().getFirstBatch();
            if (!firstBatch.hasNext()) {
                newConsistent = false;
            }
            else {
                BsonDocument doc = firstBatch.next();
                BsonValue consistentField = doc.get("consistent");
                newConsistent = consistentField != null && consistentField.isBoolean()
                        && consistentField.asBoolean().getValue();
            }
            consistent = newConsistent;
        }
    }

    private void loadStoredConfig() {
        LOGGER.warn("loadStoredConfig() is not implemented yet");
    }

    public static interface ReplCoordinatorOwnerCallback {
        public void replCoordStopped();
    }

    @NotThreadSafe
    private abstract class MyMemberStateInteface implements MemberStateInterface {
        private final Lock lock;
        private boolean closed;
        private final boolean canChangeState;

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
        public BsonObjectId getOurElectionId() {
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
    }

    private class RecoveryServiceCallback implements RecoveryService.Callback {

        @Locked(exclusive = true)
        private void stopRecoveryMode() {
            stopRecoveryModeAsync();
            awaitRecoveryStopped();
        }

        @Override
        public void recoveryFinished() {
            executor.execute(() -> {
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
            });
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
            Status<?> status = ReplCoordinator.this.setConsistentState(consistent);
            if (!status.isOk()) {
                LOGGER.error("Fatal error: It was impossible to store the consistent state: {}", status);
                throw new AssertionError("Fatal error: It was impossible to store the consistent state: " + status);
            }
        }

        @Override
        public boolean canAcceptWrites(String database) {
            return true;
        }
    }

    private class OplogReplierCallback implements OplogApplierService.Callback {

        private volatile boolean shuttingUp = false;

        @Locked(exclusive = true)
        private void stopSecondaryMode() {
            Preconditions.checkState(memberState != null
                    && memberState.equals(MemberState.RS_SECONDARY));

            assert oplogReplierService != null;
            shuttingUp = true;

            stopSecondaryModeAsync();
            awaitSecondaryStopped();
        }

        @Override
        public void rollback(RollbackReplicationException t) {
            LOGGER.debug("Secondary request a rollback with an exception", t);
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
        public void onFinish() {
            if (!shuttingUp) {
                LOGGER.error("Unexpected oplog applier service shutdown. Trying to transist to"
                        + "unrecoverable mode.");
                executor.execute(() -> {
                    Lock writeLock = lock.writeLock();
                    writeLock.lock();
                    try {
                        if (MemberState.RS_SECONDARY.equals(memberState)) {
                            stopSecondaryMode();
                        }
                        startUnrecoverableMode();
                    } finally {
                        writeLock.unlock();
                    }
                });
            }
        }

        @Override
        public void onError(Throwable t) {
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
