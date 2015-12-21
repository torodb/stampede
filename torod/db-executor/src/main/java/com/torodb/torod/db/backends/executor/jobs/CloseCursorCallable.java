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
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UserToroException;
import javax.inject.Inject;

/**
 *
 */
public class CloseCursorCallable extends Job<Void> {

    private final DbWrapper dbWrapper;
    private final CursorId cursorId;
    private final Report report;

    @Inject
    public CloseCursorCallable(
            DbWrapper dbWrapper, 
            Report report, 
            CursorId cursorId) {
        this.dbWrapper = dbWrapper;
        this.cursorId = cursorId;
        this.report = report;
    }

    @Override
    protected Void failableCall() throws ToroException, ToroRuntimeException {
        try {
            Cursor cursor = dbWrapper.getGlobalCursor(cursorId);
            
            cursor.close();
            
            report.closeCursorExecuted(cursorId);
            
            return null;
        }
        catch (IllegalArgumentException ex) {
            throw new UserToroException(ex);
        }
    }

    @Override
    protected Void onFail(Throwable t) throws ToroException,
            ToroRuntimeException {
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        throw new ToroRuntimeException(t);
    }
    
    public static interface Report {
        
        public void closeCursorExecuted(CursorId cursorId);
    }

}
