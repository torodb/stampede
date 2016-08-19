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
import com.torodb.core.backend.WriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;

/**
 *
 */
public class WriteInternalTransaction implements InternalTransaction {
    private final MetainfoRepository metainfoRepository;
    private MutableMetaSnapshot metaSnapshot;
    private final WriteBackendTransaction backendTransaction;

    private WriteInternalTransaction(MetainfoRepository metainfoRepository, MutableMetaSnapshot metaSnapshot, WriteBackendTransaction backendConnection) {
        this.metainfoRepository = metainfoRepository;
        this.metaSnapshot = metaSnapshot;
        this.backendTransaction = backendConnection;
    }

    static WriteInternalTransaction createWriteTransaction(BackendConnection backendConnection, MetainfoRepository metainfoRepository) {
        try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
            
            MutableMetaSnapshot snapshot = snapshotStage.createMutableSnapshot();

            return new WriteInternalTransaction(metainfoRepository, snapshot, backendConnection.openWriteTransaction());
        }
    }

    public WriteBackendTransaction getBackendConnection() {
        return backendTransaction;
    }

    @Override
    public WriteBackendTransaction getBackendTransaction() {
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
        backendTransaction.close();
    }

}
