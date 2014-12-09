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
import com.torodb.torod.db.executor.report.FindCollectionsReport;
import java.util.Map;
import javax.inject.Inject;

/**
 *
 */
public class FindCollectionsCallable extends SystemDbCallable<Map<String, Integer>> {

    private final FindCollectionsReport report;
    
    @Inject
    public FindCollectionsCallable(
            DbWrapper dbWrapperPool, 
            FindCollectionsReport report) {
        super(dbWrapperPool);
        this.report = report;
    }

    @Override
    Map<String, Integer> call(DbConnection db) throws ImplementationDbException {
        Map<String, Integer> result = db.findCollections();
        report.taskExecuted();
        return result;
    }

    @Override
    void doCallback() {
    }

}
