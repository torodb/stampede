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
import javax.inject.Inject;
import javax.sql.DataSource;

/**
 *
 */
public class InternalTransactionManager {

    private final DataSource ds;
    private final MetainfoRepository metainfoRepository;

    @Inject
    public InternalTransactionManager(DataSource ds, MetainfoRepository metainfoRepository) {
        this.ds = ds;
        this.metainfoRepository = metainfoRepository;
    }

    public ReadOnlyInternalTransaction openReadTransaction() {
        return ReadOnlyInternalTransaction.createReadOnlyTransaction(ds, metainfoRepository);
    }

    public WriteInternalTransaction openWriteTransaction() {
        return WriteInternalTransaction.createWriteTransaction(ds, metainfoRepository);
    }

}
