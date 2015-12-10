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

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.dbWrapper.exceptions.UserDbException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 *
 */
public class QueryCallable extends Job<Void> {

    private final DbWrapper dbWrapper;
    private final String collection;
    private final CursorId cursorId;
    private final QueryCriteria filter;
    private final Projection projection;
    private final int maxResults;
    private final Report report;
    private Cursor cursor = null;

    @Inject
    public QueryCallable(
            @Nonnull DbWrapper dbWrapper,
            @Nonnull Report report,
            @Nonnull String collection, 
            @Nonnull CursorId cursorId, 
            @Nullable QueryCriteria filter, 
            @Nullable Projection projection,
            @Nonnegative int maxResults) {
        
        this.dbWrapper = dbWrapper;
        this.collection = collection;
        this.cursorId = cursorId;
        this.filter = filter;
        this.projection = projection;
        this.maxResults = maxResults;
        this.report = report;
    }

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        try {
            cursor = dbWrapper.openGlobalCursor(
                    collection,
                    cursorId,
                    filter,
                    projection,
                    maxResults
            );
            report.queryExecuted(
                    collection, 
                    cursorId, 
                    filter, 
                    projection, 
                    maxResults
            );
        }
        catch (ImplementationDbException ex) {
            throw new ToroImplementationException(ex);
        }
        catch (UserDbException ex) {
            throw new UserToroException(ex);
        }
        return null;
    }

    @Override
    protected Void onFail(Throwable t) throws ToroException,
            ToroRuntimeException {
        if (cursor != null) {
            cursor.close();
        }
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        throw new ToroRuntimeException(t);
    }

    public static interface Report {
        public void queryExecuted(
            @Nonnull String collection, 
            @Nonnull CursorId cursorId, 
            @Nullable QueryCriteria filter, 
            @Nullable Projection projection,
            @Nonnegative int maxResults);
    }
}
