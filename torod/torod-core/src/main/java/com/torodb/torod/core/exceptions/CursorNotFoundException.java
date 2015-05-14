
package com.torodb.torod.core.exceptions;

import com.torodb.torod.core.cursors.CursorId;

/**
 *
 */
public class CursorNotFoundException extends ToroException {
    private static final long serialVersionUID = 1L;

    private final CursorId cursorId;

    public CursorNotFoundException(CursorId cursorId) {
        this.cursorId = cursorId;
    }

    public CursorNotFoundException(CursorId cursorId, String message) {
        super(message);
        this.cursorId = cursorId;
    }

    public CursorNotFoundException(CursorId cursorId, String message, Throwable cause) {
        super(message, cause);
        this.cursorId = cursorId;
    }

    public CursorNotFoundException(CursorId cursorId, Throwable cause) {
        super(cause);
        this.cursorId = cursorId;
    }

    public CursorId getCursorId() {
        return cursorId;
    }
    
}
