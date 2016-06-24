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

import java.util.Collection;
import java.util.Iterator;

import javax.annotation.Nonnull;

public interface DidCursor extends Iterator<Integer>, AutoCloseable {
    
    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    default boolean hasNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * Gets the did of the current position of this {@code DidCursor}.
     *
     * @return a did value
     */
    @Override
    Integer next();
    
    /**
     * Gets up to maxSize dids from the current position of this {@code DidCursor}.
     *
     * @return a collection of did values
     */
    @Nonnull Collection<Integer> getNextBatch(int maxSize);
    
    /**
     * Gets remaining dids from the current position of this {@code DidCursor}.
     *
     * @return a collection of did values
     */
    @Nonnull Collection<Integer> getRemaining();
    
    /**
     * Releases this {@code DidCursor} object's resources immediately.
     */
    @Override
    void close();
}
