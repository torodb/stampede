/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
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
public class ReadOnlyInternalTransaction implements InternalReadTransaction {

    private final ReadOnlyBackendTransaction backendTransaction;
    private final ImmutableMetaSnapshot metaSnapshot;

    private ReadOnlyInternalTransaction(ImmutableMetaSnapshot metaSnapshot, ReadOnlyBackendTransaction backendTransaction) {
        this.metaSnapshot = metaSnapshot;
        this.backendTransaction = backendTransaction;
    }

    static ReadOnlyInternalTransaction createReadOnlyTransaction(BackendConnection backendConnection, MetainfoRepository metainfoRepository) {
        try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
            ImmutableMetaSnapshot snapshot = snapshotStage.createImmutableSnapshot();

            return new ReadOnlyInternalTransaction(snapshot, backendConnection.openReadOnlyTransaction());
        }
    }

    @Override
    public ImmutableMetaSnapshot getMetaSnapshot() {
        return metaSnapshot;
    }

    @Override
    public void close() {
        backendTransaction.close();
    }

}
