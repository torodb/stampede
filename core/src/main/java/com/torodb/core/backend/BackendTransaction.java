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

import com.google.common.collect.Multimap;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.ToroCursor;
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

    public ToroCursor fetch(MetaDatabase db, MetaCollection col, Cursor<Integer> didCursor);

    public ToroCursor findAll(MetaDatabase db, MetaCollection col);

    public ToroCursor findByField(MetaDatabase db, MetaCollection col,
            MetaDocPart docPart, MetaField field, KVValue<?> value);

    /**
     * Return a cursor that iterates over all documents that fulfill the query.
     *
     * Each entry on the metafield is a value restriction on the entry metafield. The query is
     * fulfilled if for at least one entry, the evaluation is true.
     * @param db
     * @param col
     * @param docPart
     * @param valuesMultimap
     * @return
     */
    public ToroCursor findByFieldIn(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
            Multimap<MetaField, KVValue<?>> valuesMultimap);

    public void rollback();

    @Override
    public void close();
}