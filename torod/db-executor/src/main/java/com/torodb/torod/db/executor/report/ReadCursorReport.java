
package com.torodb.torod.db.executor.report;

import com.torodb.torod.core.cursors.CursorId;

/**
 *
 */
public interface ReadCursorReport {

    public void taskExecuted(CursorId cursorId, int maxResult);
}
