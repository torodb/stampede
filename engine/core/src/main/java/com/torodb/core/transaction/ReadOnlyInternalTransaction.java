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
import com.torodb.core.backend.ReadOnlyBackendTransaction;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;

/**
 *
 */
public class ReadOnlyInternalTransaction implements InternalTransaction {

  private final ReadOnlyBackendTransaction backendTransaction;
  private final ImmutableMetaSnapshot metaSnapshot;

  private ReadOnlyInternalTransaction(ImmutableMetaSnapshot metaSnapshot,
      ReadOnlyBackendTransaction backendTransaction) {
    this.metaSnapshot = metaSnapshot;
    this.backendTransaction = backendTransaction;
  }

  static ReadOnlyInternalTransaction createReadOnlyTransaction(BackendConnection backendConnection,
      MetainfoRepository metainfoRepository) {
    try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
      ImmutableMetaSnapshot snapshot = snapshotStage.createImmutableSnapshot();

      return new ReadOnlyInternalTransaction(snapshot, backendConnection.openReadOnlyTransaction());
    }
  }

  @Override
  public ReadOnlyBackendTransaction getBackendTransaction() {
    return backendTransaction;
  }

  @Override
  public ImmutableMetaSnapshot getMetaSnapshot() {
    return metaSnapshot;
  }

  @Override
  public void rollback() {
  }

  @Override
  public void close() {
    backendTransaction.close();
  }

}
