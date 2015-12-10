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

package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.SystemExecutor;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class ReserveSubDocIdsCallable extends SystemDbCallable<Void> {

    private final String collection;
    private final int idsToReserve;
    @Nullable 
    private final SystemExecutor.ReserveDocIdsCallback callback;
    private final Report report;

    @Inject
    public ReserveSubDocIdsCallable(
            DbWrapper dbWrapperPool,
            String collection,
            int idsToReserve,
            SystemExecutor.ReserveDocIdsCallback callback,
            Report report) {

        super(dbWrapperPool);
        this.collection = collection;
        this.idsToReserve = idsToReserve;
        this.callback = callback;
        this.report = report;
    }

    @Override
    Void call(DbConnection db) throws ImplementationDbException {
        db.reserveDocIds(collection, idsToReserve);

        return null;
    }

    @Override
    void doCallback(Void result) {
        if (callback != null) {
            callback.reservedDocIds(collection, idsToReserve);
        }
        report.reserveSubDocIdsExecuted(collection, idsToReserve);
    }
    
    public static interface Report {
        public void reserveSubDocIdsExecuted(String collection, int reservedIds);
    }
}
