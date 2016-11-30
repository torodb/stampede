/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.metainfo.cache.mvcc;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.core.transaction.metainf.UnmergeableException;
import com.torodb.core.transaction.metainf.WrapperMutableMetaSnapshot;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.Nonnull;
import javax.inject.Inject;

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
    } else {
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

    private boolean assertCheck(ImmutableMetaSnapshot currentSnapshot,
        MutableMetaSnapshot newSnapshot) {
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
