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

package com.torodb.torod.db.backends.sql;

import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.exceptions.UnknownMaxElementsException;
import com.torodb.torod.core.subdocument.SplitDocument;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class EmptyCursor implements Cursor {

    private boolean closed = false;
    
    @Override
    public List<SplitDocument> readDocuments(int maxResults) 
            throws CursorException {
        if (closed) {
            throw new IllegalArgumentException("Closed cursor");
        }
        return Collections.emptyList();
    }

    @Override
    public List<SplitDocument> readAllDocuments() throws CursorException {
        if (closed) {
            throw new IllegalArgumentException("Closed cursor");
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public int getMaxElements() throws UnknownMaxElementsException {
        return 0;
    }
    
}
