
package com.torodb.torod.mongodb.repl;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.MemberState;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import com.mongodb.WriteConcern;
import com.torodb.torod.mongodb.annotations.Locked;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Provider;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@ThreadSafe
public class ReplCoordinator extends AbstractIdleService implements ReplInterface {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(ReplCoordinator.class);
    private final ReadWriteLock lock;
    private final Provider<OplogReader> oplogReaderProvider;
    private volatile MemberState memberState;
    private final Executor executor;
    private final OplogManager oplogManager;
    private final ObjectId myRID;
    private final int myId;

    private RecoveryService recoveryService;
    private SecondaryStateService secondaryService;

    @Inject
    public ReplCoordinator(
            Provider<OplogReader> oplogReaderProvider,
            Executor replExecutor) {
        this.executor = replExecutor;
        this.oplogReaderProvider = oplogReaderProvider;
        this.oplogManager = new OplogManager();
        
        this.lock = new ReentrantReadWriteLock();

        recoveryService = null;
        secondaryService = null;
        memberState = null;

        this.myId = 123;
        this.myRID = new ObjectId();
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected void startUp() throws Exception {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            //TODO: temporal implementation

            startRecoveryMode();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Request to shutdown replication");
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
                oplogReaderProvider.get(),
                executor
        );
        recoveryService.startAsync();
    }

    @Locked(exclusive = true)
    private void stopRecoveryMode() {
        LOGGER.info("Stopping RECOVERY mode");
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
        secondaryService = new SecondaryStateService(new SecondaryServiceCallback(), oplogManager, oplogReaderProvider.get(), executor);

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
        secondaryService.shutDown();
        secondaryService.awaitTerminated();
        secondaryService = null;
    }

    @Locked(exclusive = true)
    private void startRollbackMode() {
        LOGGER.warn("Rollback request ignored. Starting recovery");
        startRecoveryMode();
    }

    @NotThreadSafe
    private abstract class MyMemberStateInteface implements MemberStateInterface {
        private final Lock lock;
        private final boolean writePermission;
        private boolean closed;

        public MyMemberStateInteface(Lock lock, boolean writePermission) {
            this.lock = lock;
            this.writePermission = writePermission;
            this.closed = false;
        }

        @Override
        public boolean hasWritePermission() {
            return writePermission;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                lock.unlock();
            }
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

    }

    @NotThreadSafe
    private class MyPrimaryStateInterface extends MyMemberStateInteface implements PrimaryStateInterface {

        public MyPrimaryStateInterface(Lock lock, boolean writePermission) {
            super(lock, writePermission);
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

    }

    private class MyRecoveryStateInterface extends MyMemberStateInteface implements RecoveryStateInterface {

        public MyRecoveryStateInterface(Lock lock, boolean writePermission) {
            super(lock, writePermission);
        }

        @Override
        public MemberState getMemberState() {
            return MemberState.RS_RECOVERING;
        }

        @Override
        public boolean canNodeAcceptWrites(String database) {
            return database.startsWith("local.");
        }

    }

    @ThreadSafe
    private class MySecondaryStateInferface extends MyMemberStateInteface implements SecondaryStateInferface {
        public MySecondaryStateInferface(Lock lock, boolean writePermission) {
            super(lock, writePermission);
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
                if (memberState.equals(MemberState.RS_RECOVERING)) {
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

    }

    private class SecondaryServiceCallback implements SecondaryStateService.Callback {

        @Override
        public void rollbackRequired() {
            Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                if (memberState.equals(MemberState.RS_SECONDARY)) {
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

    }
}
