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

import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.SharedWriteBackendTransaction;
import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;

/**
 *
 */
public class SharedWriteInternalTransaction extends WriteInternalTransaction<SharedWriteBackendTransaction> {
    private SharedWriteInternalTransaction(MetainfoRepository metainfoRepository, MutableMetaSnapshot metaSnapshot, SharedWriteBackendTransaction backendTransaction, ReadLock readLock) {
        super(metainfoRepository, metaSnapshot, backendTransaction, readLock);
    }

    static SharedWriteInternalTransaction createSharedWriteTransaction(BackendConnection backendConnection, MetainfoRepository metainfoRepository) {
        ReadLock sharedLock = sharedLock();
        sharedLock.lock();
        try {
            return createWriteTransaction(metainfoRepository, snapshot -> new SharedWriteInternalTransaction(metainfoRepository, snapshot, backendConnection.openSharedWriteTransaction(), sharedLock));
        } catch(Throwable throwable) {
            sharedLock.unlock();
            throw throwable;
        }
    }
}
