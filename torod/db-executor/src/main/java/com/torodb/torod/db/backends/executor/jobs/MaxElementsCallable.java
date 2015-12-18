
package com.torodb.torod.db.backends.executor.jobs;

import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.dbWrapper.DbWrapper;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;

/**
 *
 */
public class MaxElementsCallable extends Job<Integer> {

    private final DbWrapper dbWrapper;
    private final CursorId cursorId;
    private final MaxElementsCallable.Report report;

    public MaxElementsCallable(DbWrapper dbWrapper, Report report, CursorId cursorId) {
        this.dbWrapper = dbWrapper;
        this.cursorId = cursorId;
        this.report = report;
    }
    
    @Override
    protected Integer failableCall() throws ToroException,
            ToroRuntimeException {
        
        Cursor cursor = dbWrapper.getGlobalCursor(cursorId);
        int maxElements = cursor.getMaxElements();
        
        report.maxElementsExecuted(cursorId, maxElements);
        
        return maxElements;
    }

    @Override
    protected Integer onFail(Throwable t) throws
            ToroException, ToroRuntimeException {
        if (t instanceof ToroException) {
            throw (ToroException) t;
        }
        throw new ToroRuntimeException(t);
    }
    
    public static interface Report {
        public void maxElementsExecuted(
                CursorId cursorId, 
                int result);
    }

}
