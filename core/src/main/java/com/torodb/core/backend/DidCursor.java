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

package com.torodb.core.backend;

public interface DidCursor {
    
    /**
     * Retrieves whether the cursor is after the last did in
     * this <code>DidCursor</code> object.
     *
     * @return <code>true</code> if the cursor is after the last did;
     * <code>false</code> if the cursor is at any other position or the
     * cursor contains no dids
     */
    boolean isAfterLast();
    
    /**
     * Retrieves whether the cursor is before the first did in
     * this <code>DidCursor</code> object.
     *
     * @return <code>true</code> if the cursor is before the first did;
     * <code>false</code> if the cursor is at any other position or the
     * cursor contains no dids
     */
    boolean isBeforeFirst();
    
    /**
     * Moves the cursor forward one did from its current position.
     * A <code>DidCursor</code> cursor is initially positioned
     * before the first did; the first call to the method
     * <code>next</code> makes the first did the current did; the
     * second call makes the second did the current did, and so on.
     * <p>
     * When a call to the <code>next</code> method returns <code>false</code>,
     * the cursor is positioned after the last did. Any
     * invocation of a <code>DidCursor</code> method which requires a
     * current did will result in a <code>IllegalStateException</code> being thrown.
     *
     * @return <code>true</code> if the new current did is valid;
     * <code>false</code> if there are no more dids
     */
    boolean next();
    
    /**
     * <p>Gets the value of the current did
     * of this <code>DidCursor</code>.
     *
     * @return a did value
     */
    int get();
    
    /**
     * Releases this <code>DidCursor</code> object's resources immediately.
     */
    void close();
}
