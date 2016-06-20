
package com.torodb.metainfo.cache.mvcc;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot.ImmutableMetaSnapshotFactory;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import com.torodb.core.transaction.metainf.utils.DefaultMergeChecker;

/**
 *
 */
public class MvccMetainfoRepository implements MetainfoRepository {

    private static final Logger LOGGER = LogManager.getLogger(MvccMetainfoRepository.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ImmutableMetaSnapshot currentSnapshot;
    private final MergeChecker mergeChecker;

    public MvccMetainfoRepository() {
        this.currentSnapshot = new ImmutableMetaSnapshot.Builder().build();
        mergeChecker = DefaultMergeChecker::checkMerge;
    }

    public MvccMetainfoRepository(ImmutableMetaSnapshot currentView) {
        this.currentSnapshot = currentView;
        mergeChecker = DefaultMergeChecker::checkMerge;
    }

    @Inject
    public MvccMetainfoRepository(ImmutableMetaSnapshotFactory factory) {
        this(factory.getImmutableMetaSnapshot());
    }

    @Override
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
    public MergerStage startMerge(MutableMetaSnapshot newSnapshot) throws UnmergeableException {
        LOGGER.trace("Trying to create a {}", MvccMergerStage.class);
        lock.writeLock().lock();
        MvccMergerStage mergeStage = null;
        try {
            mergeChecker.checkMerge(currentSnapshot, newSnapshot);
            mergeStage = new MvccMergerStage(newSnapshot, lock.writeLock());
            LOGGER.trace("{} created", MvccMergerStage.class);
        } finally {
            if (mergeStage == null) {
                LOGGER.error("Error while trying to create a {}", MvccMergerStage.class);
                lock.writeLock().unlock();
            }
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
        private boolean cancelled = false;

        public MvccMergerStage(MutableMetaSnapshot changedView, WriteLock writeLock) {
            this.changedSnapshot = changedView;
            this.writeLock = writeLock;
        }

        private MvccMetainfoRepository getOwner() {
            return MvccMetainfoRepository.this;
        }

        @Override
        public MetainfoRepository getAssociatedRepository() {
            Preconditions.checkState(open, "This stage is closed");
            return MvccMetainfoRepository.this;
        }

        @Override
        public void cancel() {
            Preconditions.checkState(open, "This stage is closed");
            cancelled = true;
        }

        @Override
        public void close() {
            if (open) {
                open = false;
                if (!cancelled) {
                    Preconditions.checkState(lock.writeLock().isHeldByCurrentThread(), "Trying to "
                            + "apply changes without holding the write lock");

                    assertCheck(currentSnapshot, changedSnapshot);

                    MetaSnapshotMergeBuilder merger = new MetaSnapshotMergeBuilder(currentSnapshot);
                    for (MutableMetaDatabase modifiedDatabase : changedSnapshot.getModifiedDatabases()) {
                        merger.addModifiedDatabase(modifiedDatabase);
                    }
                    MvccMetainfoRepository.this.currentSnapshot = merger.build();
                }

                writeLock.unlock();
            }
        }

        private void assertCheck(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot) {
            try {
                boolean assertsEnabled = false;
                assert assertsEnabled = true; // Intentional side effect!!!
                if (assertsEnabled) {
                    mergeChecker.checkMerge(currentSnapshot, newSnapshot);
                }
            } catch (UnmergeableException ex) {
                throw new AssertionError("Unmergeable changes", ex);
            }
        }

    }

    @NotThreadSafe
    public static interface MergeChecker {
        public void checkMerge(ImmutableMetaSnapshot currentSnapshot, MutableMetaSnapshot newSnapshot) throws UnmergeableException;
    }
}
