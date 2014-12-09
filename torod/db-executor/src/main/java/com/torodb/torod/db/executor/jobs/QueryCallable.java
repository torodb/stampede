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

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.db.executor.report.QueryReport;
import java.util.concurrent.Callable;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class QueryCallable implements Callable<Void> {

    private final DbWrapper dbWrapper;
    private final String collection;
    private final CursorId cursorId;
    private final QueryCriteria filter;
    private final Projection projection;
    private final int maxResults;
    private final QueryReport report;

    @Inject
    public QueryCallable(
            @Nonnull DbWrapper dbWrapper,
            @Nonnull String collection, 
            @Nonnull CursorId cursorId, 
            @Nullable QueryCriteria filter, 
            @Nullable Projection projection,
            @Nonnegative int maxResults,
            QueryReport report) {
        
        this.dbWrapper = dbWrapper;
        this.collection = collection;
        this.cursorId = cursorId;
        this.filter = filter;
        this.projection = projection;
        this.maxResults = maxResults;
        this.report = report;
    }

    @Override
    public Void call() throws ImplementationDbException, UserDbException {
        dbWrapper.openGlobalCursor(
                collection, 
                cursorId, 
                filter, 
                projection, 
                maxResults
        );
        report.taskExecuted(
                collection, 
                cursorId, 
                filter, 
                projection, 
                maxResults, 
                dbWrapper.getGlobalCursor(cursorId).countRemainingDocs()
        );
        return null;
    }

}
