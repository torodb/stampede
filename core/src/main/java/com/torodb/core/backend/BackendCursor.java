
package com.torodb.core.backend;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;

/**
 *
 */
public interface BackendCursor {

    /**
     * Returns a cursor with <em>living</em> {@link DocPartResult doc part results}.
     *
     * This doc part results <b>must be closed</b> once they are not going to
     * be used.
     * @return
     */
    public Cursor<DocPartResult> asDocPartResultCursor();

    public Cursor<Integer> asDidCursor();

}
