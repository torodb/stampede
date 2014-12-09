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

import com.torodb.torod.core.dbWrapper.DbConnection;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.executor.SystemExecutor;
import com.torodb.torod.db.executor.report.CreateCollectionReport;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class CreateCollectionCallable extends SystemDbCallable<Void> {

    private final String collection;
    @Nullable
    private final SystemExecutor.CreateCollectionCallback callback;
    private final CreateCollectionReport report;

    @Inject
    public CreateCollectionCallable(
            DbWrapper dbWrapperPool,
            String collection, 
            SystemExecutor.CreateCollectionCallback callback, 
            CreateCollectionReport report) {
        super(dbWrapperPool);
        this.collection = collection;
        this.callback = callback;
        this.report = report;
    }

    @Override
    Void call(DbConnection db) throws ImplementationDbException {
        db.createCollection(collection);

        return null;
    }

    @Override
    void doCallback() {
        if (callback != null) {
            callback.createdCollection(collection);
        }
        report.taskExecuted(collection);
    }
}
