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

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;

import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 *
 */
public class SharedWriteInternalTransaction
    extends WriteInternalTransaction<SharedWriteBackendTransaction> {

  private SharedWriteInternalTransaction(MetainfoRepository metainfoRepository,
      MutableMetaSnapshot metaSnapshot, SharedWriteBackendTransaction backendTransaction,
      ReadLock readLock) {
    super(metainfoRepository, metaSnapshot, backendTransaction, readLock);
  }

  static SharedWriteInternalTransaction createSharedWriteTransaction(
      BackendConnection backendConnection, MetainfoRepository metainfoRepository) {
    ReadLock sharedLock = sharedLock();
    sharedLock.lock();
    try {
      return createWriteTransaction(metainfoRepository, snapshot ->
          new SharedWriteInternalTransaction(metainfoRepository, snapshot, backendConnection
              .openSharedWriteTransaction(), sharedLock));
    } catch (Throwable throwable) {
      sharedLock.unlock();
      throw throwable;
    }
  }
}
