
package com.torodb.metainfo.cache.mvcc;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.metainfo.cache.WrapperMutableMetaSnapshot;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class MvccMetainfoRepository implements MetainfoRepository {

    private static final Logger LOGGER = LogManager.getLogger(MvccMetainfoRepository.class);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ImmutableMetaSnapshot currentSnapshot;

    public MvccMetainfoRepository() {
        this.currentSnapshot = new ImmutableMetaSnapshot.Builder().build();
    }

    public MvccMetainfoRepository(ImmutableMetaSnapshot currentView) {
        this.currentSnapshot = currentView;
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
    public MergerStage startMerge(MutableMetaSnapshot stage) {
        LOGGER.trace("Trying to create a {}", MvccMergerStage.class);
        lock.writeLock().lock();
        MvccMergerStage mergeStage = null;
        try {
            mergeStage = new MvccMergerStage(stage, lock.writeLock());
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

                    MvccMetainfoRepository.this.currentSnapshot = changedSnapshot.immutableCopy();
                }

                writeLock.unlock();
            }
        }

    }

}
