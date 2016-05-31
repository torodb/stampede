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

import com.torodb.core.transaction.metainf.MetainfoRepository;
import com.torodb.core.transaction.metainf.MetainfoRepository.MergerStage;
import com.torodb.core.transaction.metainf.MetainfoRepository.SnapshotStage;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 *
 */
public class WriteInternalTransaction implements InternalReadTransaction {
    private final MetainfoRepository metainfoRepository;
    private final MutableMetaSnapshot metaSnapshot;
    private final Connection connection;

    private WriteInternalTransaction(MetainfoRepository metainfoRepository, MutableMetaSnapshot metaSnapshot, Connection connection) {
        this.metainfoRepository = metainfoRepository;
        this.metaSnapshot = metaSnapshot;
        this.connection = connection;
    }

    static WriteInternalTransaction createWriteTransaction(DataSource ds, MetainfoRepository metainfoRepository) throws ToroTransactionException {
        try (SnapshotStage snapshotStage = metainfoRepository.startSnapshotStage()) {
            Connection conn = ds.getConnection();
            MutableMetaSnapshot snapshot = snapshotStage.createMutableSnapshot();

            return new WriteInternalTransaction(metainfoRepository, snapshot, conn);
        } catch (SQLException ex) {
            throw new ToroTransactionException(ex);
        }
    }

    @Override
    public MutableMetaSnapshot getMetainfoView() {
        return metaSnapshot;
    }

    @Override
    public void close() throws ToroTransactionException {
        try (MergerStage mergeStage = metainfoRepository.startMerge(metaSnapshot)) {
            connection.commit();
            connection.close();
        } catch (SQLException ex) {
            //TODO: Decide if we should
            throw new ToroTransactionException(ex);
        }
    }

}
