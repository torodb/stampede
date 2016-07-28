
package com.torodb.metainfo.cache.mvcc;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MvccMetainfoRepository implements MetainfoRepository {

    private static final Logger LOGGER = LogManager.getLogger(MvccMetainfoRepository.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ImmutableMetaSnapshot currentSnapshot;
    private NoChangeMergeStage noChangeMergeStage = new NoChangeMergeStage();

    @Inject
    public MvccMetainfoRepository() {
        this.currentSnapshot = new ImmutableMetaSnapshot.Builder().build();
    }

    public MvccMetainfoRepository(ImmutableMetaSnapshot currentView) {
        this.currentSnapshot = currentView;
    }

    @Override
    @Nonnull
    @SuppressFBWarnings(value = {"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", "UL_UNRELEASED_LOCK"})
    public SnapshotStage startSnapshotStage() {
        ReadLock readLock = lock.readLock();
        LOGGER.trace("Trying to create a {}", MvccSnapshotStage.class);
        readLock.lock();
        SnapshotStage snapshotStage = null;
        try {
            snapshotStage = new MvccSnapshotStage(readLock);
            LOGGER.trace("{} created", MvccSnapshotStage.class);
        } finally {
            if (snapshotStage == null) {
                LOGGER.error("Error while trying to create a {}", MvccMergerStage.class);
                readLock.unlock();
            }
        }
        assert snapshotStage != null;
        return snapshotStage;
    }

    @Override
    @Nonnull
    @SuppressFBWarnings(value = {"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE", "UL_UNRELEASED_LOCK"})
    public MergerStage startMerge(MutableMetaSnapshot newSnapshot) throws UnmergeableException {
        LOGGER.trace("Trying to create a {}", MvccMergerStage.class);

        MergerStage mergeStage = null;
        if (newSnapshot.hasChanged()) {
            lock.writeLock().lock();
        
            try {
                mergeStage = new MvccMergerStage(newSnapshot, lock.writeLock());
                LOGGER.trace("{} created", MvccMergerStage.class);
            } finally {
                if (mergeStage == null) {
                    LOGGER.error("Error while trying to create a {}", MvccMergerStage.class);
                    lock.writeLock().unlock();
                }
            }
        }
        else {
            mergeStage = noChangeMergeStage;
        }
        assert mergeStage != null;
        return mergeStage;
    }

    private class MvccSnapshotStage implements SnapshotStage {

        private final ReentrantReadWriteLock.ReadLock readLock;
        private boolean open = true;

        public MvccSnapshotStage(ReentrantReadWriteLock.ReadLock readLock) {
            this.readLock = readLock;
        }

        @Override
        public ImmutableMetaSnapshot createImmutableSnapshot() {
            Preconditions.checkState(open, "This stage is closed");
            return currentSnapshot;
        }

        @Override
        public MutableMetaSnapshot createMutableSnapshot() {
            Preconditions.checkState(open, "This stage is closed");
            return new WrapperMutableMetaSnapshot(createImmutableSnapshot());
        }

        @Override
        public void close() {
            if (open) {
                open = false;
                readLock.unlock();
            }
        }

    }

    private class MvccMergerStage implements MergerStage {

        private final MutableMetaSnapshot changedSnapshot;
        private final ReentrantReadWriteLock.WriteLock writeLock;
        private boolean open = true;
        private final ImmutableMetaSnapshot.Builder snapshotBuilder;

        public MvccMergerStage(MutableMetaSnapshot changedView, WriteLock writeLock) {
            this.changedSnapshot = changedView;
            this.writeLock = writeLock;
            snapshotBuilder = new SnapshotMerger(currentSnapshot, changedView)
                            .merge();
        }

        @Override
        public void commit() {
            Preconditions.checkState(open, "This stage is already closed");
            Preconditions.checkState(lock.writeLock().isHeldByCurrentThread(), "Trying to "
                    + "apply changes without holding the write lock");

            assert assertCheck(currentSnapshot, changedSnapshot);
            
            MvccMetainfoRepository.this.currentSnapshot = snapshotBuilder.build();
        }

        @Override
        public void close() {
            if (open) {
                open = false;

                writeLock.unlock();
            }
        }

        private boolean assertCheck(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot) {
            try {
                new SnapshotMerger(currentSnapshot, newSnapshot)
                        .merge();
                return true;
            } catch (UnmergeableException ex) {
                throw new AssertionError("Unmergeable changes", ex);
            }
        }

    }

    private static class NoChangeMergeStage implements MergerStage {

        @Override
        public void commit() {
        }

        @Override
        public void close() {
        }

    }
}
