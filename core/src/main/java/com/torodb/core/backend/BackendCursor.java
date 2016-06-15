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

import java.io.Closeable;
import java.util.Collection;

import javax.annotation.Nonnull;

import com.torodb.core.document.ToroDocument;

/**
 *
 */
public interface BackendCursor extends Closeable {
    
    public Collection<ToroDocument> readDocuments(int maxResults) throws CursorException;
    
    @Nonnull
    public Collection<ToroDocument> readAllDocuments() throws CursorException;
    
    /**
     * Close the cursor and the connection that created it.
     */
    @Override
    public void close();

    public static class CursorException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private transient final BackendCursor backendCursor;

        public CursorException(BackendCursor backendCursor) {
            this.backendCursor = backendCursor;
        }

        public CursorException(BackendCursor backendCursor, String message) {
            super(message);
            this.backendCursor = backendCursor;
        }

        public CursorException(BackendCursor backendCursor, String message, Throwable cause) {
            super(message, cause);
            this.backendCursor = backendCursor;
        }

        public CursorException(BackendCursor backendCursor, Throwable cause) {
            super(cause);
            this.backendCursor = backendCursor;
        }

        public BackendCursor getCursor() {
            return backendCursor;
        }
    }
}
