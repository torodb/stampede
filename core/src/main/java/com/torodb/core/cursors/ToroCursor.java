
package com.torodb.core.cursors;

import com.torodb.core.document.ToroDocument;

/**
 *
 */
public interface ToroCursor {

    public Cursor<ToroDocument> asDocCursor();

    public Cursor<Integer> asDidCursor();

}
