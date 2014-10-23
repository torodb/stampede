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

package com.torodb.torod.core.dbMetaInf;

import com.torodb.torod.core.Session;
import com.torodb.torod.core.cursors.CursorProperties;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.dbWrapper.Cursor;
import java.io.Closeable;

/**
 *
 */
public interface CursorManager extends Closeable {

    /**
     * Creates a cursor.
     * <p>
     * <p>
     * @param owner
     * @param hasTimeout
     * @param autoclose
     * @return The created cursor
     */
    public CursorProperties createUnlimitedCursor(
            Session owner,
            boolean hasTimeout,
            boolean autoclose
    );
    
    /**
     * Creates a cursor.
     * <p>
     * <p>
     * @param owner
     * @param limit
     * @param hasTimeout
     * @param autoclose
     * @return The created cursor
     */
    public CursorProperties createLimitedCursor(
            Session owner,
            boolean hasTimeout,
            boolean autoclose,
            int limit
    );

    public boolean exists(CursorId cursorId);

    /**
     *
     * @param cursorId
     * @throws IllegalArgumentException if there is no managed cursor with the given id
     */
    public void close(CursorId cursorId) throws IllegalArgumentException;

    /**
     * Modifies the meta information associated with the given cursor as the given number of results had been read.
     * <p>
     * This method may close the cursor if it is autoclousable and it limit is reached. In this case, true is returned.
     * <p>
     * @param cursorId
     * @param readSize
     * @return true iff the cursor will be closed
     * @throws IllegalArgumentException if the cursor doesn't exist or if more documents than expected have been read
     * @see #notifyAllRead(com.torodb.torod.core.cursors.CursorId)
     */
    public boolean notifyRead(CursorId cursorId, int readSize) throws IllegalArgumentException;

    /**
     * Marks the given cursor as completely read.
     * <p>
     * This method will close the cursor if it is autoclousable. In this case, true is returned.
     * <p>
     * @param cursorId
     * @return true iff the cursor will be closed
     * @throws IllegalArgumentException if the cursor doesn't exist
     * @see #notifyRead(com.torodb.torod.core.cursors.CursorId, int)
     */
    public boolean notifyAllRead(CursorId cursorId) throws IllegalArgumentException;

    /**
     * Returns the number of elements that had been read from this cursor.
     * <p>
     * @param cursorId
     * @return
     * @throws IllegalArgumentException
     * @see #notifyAllRead(com.torodb.torod.core.cursors.CursorId) 
     * @see #notifyRead(com.torodb.torod.core.cursors.CursorId, int)
     */
    public int getReadElements(CursorId cursorId) throws IllegalArgumentException;

    /**
     *
     * @param cursorId
     * @return
     * @throws IllegalArgumentException if the give id doesn't correspond with an open cursor
     */
    public CursorProperties getCursor(CursorId cursorId) throws IllegalArgumentException;

    /**
     * Set the timeout in milliseconds.
     * <p>
     * When a cursor that {@linkplain Cursor#hasTimeout() has timeout} hasn't been used in the last <em>timeout</em>
     * milliseconds, the cursor can be automatically closed.
     * <p>
     * @param millis
     */
    public void setTimeout(long millis);

    /**
     *
     * Performs any pending maintenance operations needed by the cache.
     * <p>
     * Some {@link CursorManager cursor managers} automatically manage the unused cursors, but others could need an
     * external actor that calls this method.
     * <p>
     * Exactly which activities are performed -- if any -- is implementation-dependent.
     */
    public void cleanUp();
    
    public void close();

}
