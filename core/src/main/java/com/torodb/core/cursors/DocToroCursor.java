/*
 * This file is part of ToroDB.
 *
 * ToroDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ToroDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with core. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.cursors;

import com.torodb.core.document.ToroDocument;

/**
 *
 */
public class DocToroCursor implements ToroCursor {

    private final Cursor<ToroDocument> docCursor;

    public DocToroCursor(Cursor<ToroDocument> docCursor) {
        this.docCursor = docCursor;
    }

    @Override
    public Cursor<ToroDocument> asDocCursor() {
        return docCursor;
    }

    @Override
    public Cursor<Integer> asDidCursor() {
        return new TransformCursor<>(docCursor, (doc) -> doc.getId());
    }
}
