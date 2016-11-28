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

package com.torodb.core.transaction;

import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Function;

/**
 *
 */
public abstract class WriteInternalTransaction<T extends WriteBackendTransaction> implements
    InternalTransaction {

  private static final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private static final ReadLock sharedLock = globalLock.readLock();
  private static final WriteLock exclusiveLock = globalLock.writeLock();

  private final MetainfoRepository metainfoRepository;
  private MutableMetaSnapshot metaSnapshot;
  private final T backendTransaction;
  private final Lock lock;

  protected WriteInternalTransaction(MetainfoRepository metainfoRepository,
      MutableMetaSnapshot metaSnapshot, T backendConnection, Lock lock) {
    this.metainfoRepository = metainfoRepository;
    this.metaSnapshot = metaSnapshot;
    this.backendTransaction = backendConnection;
    this.lock = lock;
  }

  protected static ReadLock sharedLock() {
    return sharedLock;
  }

  protected static WriteLock exclusiveLock() {
    return exclusiveLock;
  }

  protected static <T extends WriteInternalTransaction<?>> T createWriteTransaction(
      MetainfoRepository metainfoRepository,
      Function<MutableMetaSnapshot, T> internalTransactionSupplier) {
    try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {

      MutableMetaSnapshot snapshot = snapshotStage.createMutableSnapshot();

      return internalTransactionSupplier.apply(snapshot);
    }
  }

  @Override
  public T getBackendTransaction() {
    return backendTransaction;
  }

  @Override
  public MutableMetaSnapshot getMetaSnapshot() {
    return metaSnapshot;
  }

  public void commit() throws RollbackException, UserException {
    try (MergerStage mergeStage = metainfoRepository.startMerge(metaSnapshot)) {
      backendTransaction.commit();

      mergeStage.commit();
    }
  }

  @Override
  public void rollback() {
    backendTransaction.rollback();

    //This is only correct if the SQL transaction completely rollback (ie no savepoints were used)
    //On other case, if another writer commited their chenges, we could have a disparity
    //between what we see on the metainformation (the changes of the other writer) and what we
    //see on the database (were our rollbacked transaction did not see the other writer changes)
    try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
      metaSnapshot = snapshotStage.createMutableSnapshot();
    }
  }

  @Override
  public void close() {
    try {
      backendTransaction.close();
    } finally {
      lock.unlock();
    }
  }

}
