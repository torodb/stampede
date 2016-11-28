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
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 *
 */
@ThreadSafe
public class InternalTransactionManager {

  private final MetainfoRepository metainfoRepository;

  @Inject
  public InternalTransactionManager(MetainfoRepository metainfoRepository) {
    this.metainfoRepository = metainfoRepository;
  }

  public ReadOnlyInternalTransaction openReadTransaction(BackendConnection backendConnection) {
    return ReadOnlyInternalTransaction.createReadOnlyTransaction(backendConnection,
        metainfoRepository);
  }

  public SharedWriteInternalTransaction openSharedWriteTransaction(
      BackendConnection backendConnection) {
    return SharedWriteInternalTransaction.createSharedWriteTransaction(backendConnection,
        metainfoRepository);
  }

  public ExclusiveWriteInternalTransaction openExclusiveWriteTransaction(
      BackendConnection backendConnection) {
    return ExclusiveWriteInternalTransaction.createExclusiveWriteTransaction(backendConnection,
        metainfoRepository);
  }

  public ImmutableMetaSnapshot takeMetaSnapshot() {
    try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
      return snapshotStage.createImmutableSnapshot();
    }
  }

}
