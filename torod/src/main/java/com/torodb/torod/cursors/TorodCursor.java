
package com.torodb.torod.cursors;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;

/**
 *
 */
public interface TorodCursor {

    public Cursor<ToroDocument> asDocCursor();

    public Cursor<Integer> asDidCursor();

}
