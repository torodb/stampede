/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.db.executor.jobs;

import com.google.common.base.Supplier;
import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.db.executor.DefaultSessionTransaction;
import java.util.concurrent.Callable;

/**
 *
 */
public class RollbackCallable implements Callable<Void> {
    
    private final Supplier<DbConnection> connectionProvider;

    public RollbackCallable(
            Supplier<DbConnection> connectionProvider
    ) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public Void call() throws ImplementationDbException {
        connectionProvider.get().rollback();
        
        return null;
    }
}
