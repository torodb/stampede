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

package com.torodb.core.backend;

import java.util.Collection;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public interface BackendTransaction extends AutoCloseable {

    public long getDatabaseSize(MetaDatabase db);
    
    public long countAll(MetaDatabase db, MetaCollection col);
    
    public long getCollectionSize(MetaDatabase db, MetaCollection col);
    
    public long getDocumentsSize(MetaDatabase db, MetaCollection col);

    public Cursor<ToroDocument> findAll(MetaDatabase db, MetaCollection col);

    public Cursor<ToroDocument> findByField(MetaDatabase db, MetaCollection col,
            MetaDocPart docPart, MetaField field, KVValue<?> value);

    public DidCursor findAllDids(MetaDatabase db, MetaCollection col);

    public DidCursor findDidsByField(MetaDatabase db, MetaCollection col,
            MetaDocPart docPart, MetaField field, KVValue<?> value);
    
    public Collection<ToroDocument> readDocuments(MetaDatabase db, MetaCollection col, Collection<Integer> dids);
    
    @Override
    public void close();
}