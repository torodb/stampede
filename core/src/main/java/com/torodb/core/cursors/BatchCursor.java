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

package com.torodb.core.cursors;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

public class BatchCursor<E> implements AutoCloseable, Iterator<List<E>> {

    private final Cursor<E> cursor;
    private final int size;
    
    public BatchCursor(Cursor<E> cursor, int size) {
        super();
        
        Preconditions.checkArgument(size > 0);
        
        this.cursor = cursor;
        this.size = size;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public List<E> next() {
        return cursor.getNextBatch(size);
    }

    @Override
    public void close() throws Exception {
        cursor.close();
    }
}
