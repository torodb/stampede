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
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.subdocument.SplitDocument;
import com.torodb.torod.db.executor.DefaultSessionTransaction;
import com.torodb.torod.db.executor.report.ReadCursorReport;
import java.util.List;
import java.util.concurrent.Callable;
import javax.inject.Inject;

/**
 *
 */
public class ReadCursorCallable implements Callable<List<? extends SplitDocument>> {

    private final DbWrapper dbWrapper;
    private final CursorId cursorId;
    private final int maxResult;
    private final ReadCursorReport report;

    @Inject
    public ReadCursorCallable(
            DbWrapper dbWrapper,
            CursorId cursorId,
            int maxResult,
            ReadCursorReport report
    ) {
        this.dbWrapper = dbWrapper;
        this.cursorId = cursorId;
        this.maxResult = maxResult;
        this.report = report;
    }

    @Override
    public List<? extends SplitDocument> call() throws ImplementationDbException {
        Cursor cursor = null;
        boolean closeCursor = false;
        try {
            cursor = dbWrapper.getGlobalCursor(cursorId);

            List<SplitDocument> result = cursor.readDocuments(maxResult);
            
            report.taskExecuted(cursorId, maxResult);
            
            return result;
        }
        catch (RuntimeException ex) {
            closeCursor = true;
            throw ex;
        }
        catch (Error ex) {
            closeCursor = true;
            throw ex;
        }
        catch (ImplementationDbException ex) {
            closeCursor = true;
            throw ex;
        }
        finally {
            if (closeCursor && cursor != null) {
                cursor.close();
            }
        }
    }
}
