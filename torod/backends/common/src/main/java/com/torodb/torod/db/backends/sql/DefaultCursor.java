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

import com.google.common.collect.Table;
import com.torodb.torod.core.dbWrapper.Cursor;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.exceptions.UnknownMaxElementsException;
import com.torodb.torod.core.subdocument.SplitDocument;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 *
 */
public class DefaultCursor implements Cursor {

    private final DatabaseCursorGateway databaseGateway;
    private final MyIterator dids;
    private final int maxElements;
    
    /**
     *
     * @param databaseGateway
     * @param dids
     */
    public DefaultCursor(
            @Nonnull DatabaseCursorGateway databaseGateway,
            @Nonnull Set<Integer> dids) {
        this.dids = new MyIterator(dids);
        this.databaseGateway = databaseGateway;
        this.maxElements = dids.size();
    }
    
    @Override
    public List<SplitDocument> readDocuments(int maxResults) {
        if (maxResults <= 0) {
            throw new IllegalArgumentException("max results must be at least 1, but "+maxResults+" was recived");
        }
        return privateRead(maxResults);
    }

    @Override
    public List<SplitDocument> readAllDocuments() throws CursorException {
        return privateRead(dids.getRemainingDocs());
    }
    
    private List<SplitDocument> privateRead(int maxResults) throws CursorException{
        if (!dids.hasNext() || maxResults == 0) {
            return Collections.emptyList();
        }
        if (maxResults > dids.getRemainingDocs()) {
            maxResults = dids.getRemainingDocs();
        }
        
        Integer[] requiredDocs = new Integer[maxResults];
        for (int i = 0; i < maxResults; i++) {
            assert dids.hasNext();
            requiredDocs[i] = dids.next();
        }

        return databaseGateway.readDocuments(requiredDocs);
    }

    @Override
    public int getMaxElements() throws UnknownMaxElementsException {
        return maxElements;
    }

    @Override
    public void close() {
        try {
            databaseGateway.close();
        } catch (SQLException ex) {
            //TODO: Study exceptions
            throw new ToroRuntimeException(ex);
        }
    }

    public static class MyCursorException extends com.torodb.torod.core.dbWrapper.Cursor.CursorException {
        private static final long serialVersionUID = 1L;

        private final Table<Integer, Integer, String> docMap;

        public MyCursorException(Table<Integer, Integer, String> docMap, Cursor cursor, String message) {
            super(cursor, message);
            this.docMap = docMap;
        }

        public MyCursorException(Table<Integer, Integer, String> docMap, com.torodb.torod.core.dbWrapper.Cursor cursor, Throwable cause) {
            super(cursor, cause);
            this.docMap = docMap;
        }

        public Table<Integer, Integer, String> getDocMap() {
            return docMap;
        }

    }
    
    private static class MyIterator implements Iterator<Integer> {
        private int remainingDocs;
        private final Iterator<Integer> delegate;

        public MyIterator(Collection<Integer> dids) {
            this.delegate = dids.iterator();
            remainingDocs = dids.size();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Integer next() {
            if (remainingDocs > 0) {
                remainingDocs--;
            }
            return delegate.next();
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        private int getRemainingDocs() {
            return remainingDocs;
        }
    }
    
    public static interface DatabaseCursorGateway {
        List<SplitDocument> readDocuments(Integer[] documents);
        
        /**
         * This method is called once the cursor doesn't need the connection provider anymore.
         * @throws java.sql.SQLException
         */
        void close() throws SQLException;
    }
}
