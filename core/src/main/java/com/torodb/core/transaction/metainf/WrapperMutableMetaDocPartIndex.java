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
 * along with metainfo-cache. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 * 
 */

package com.torodb.core.transaction.metainf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jooq.lambda.Seq;

import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndex.Builder;

/**
 *
 */
public class WrapperMutableMetaDocPartIndex implements MutableMetaDocPartIndex {

    private final ImmutableMetaDocPartIndex wrapped;
    /**
     * This table contains all fields contained by wrapper and all new fields
     */
    private final Map<String, ImmutableMetaDocPartIndexColumn> newColumns;
    /**
     * This list just contains the fields that have been added on this wrapper but not on the
     * wrapped object.
     */
    private final List<ImmutableMetaDocPartIndexColumn> addedColumns;
    private final Consumer<WrapperMutableMetaDocPartIndex> changeConsumer;

    public WrapperMutableMetaDocPartIndex(ImmutableMetaDocPartIndex wrapped,
            Consumer<WrapperMutableMetaDocPartIndex> changeConsumer) {
        this.wrapped = wrapped;
        
        newColumns = new HashMap<>();

        wrapped.iteratorColumns().forEachRemaining((column) ->
            newColumns.put(column.getIdentifier(), column)
        );
        addedColumns = new ArrayList<>();
        this.changeConsumer = changeConsumer;
    }

    @Override
    public ImmutableMetaDocPartIndexColumn addMetaDocPartIndexColumn(String identifier, FieldIndexOrdering ordering) throws
            IllegalArgumentException {
        if (getMetaDocPartIndexColumnByIdentifier(identifier) != null) {
            throw new IllegalArgumentException("There is another column with the identifier " + identifier);
        }

        ImmutableMetaDocPartIndexColumn newField = new ImmutableMetaDocPartIndexColumn(size(), identifier, ordering);
        newColumns.put(identifier, newField);
        addedColumns.add(newField);
        changeConsumer.accept(this);
        return newField;
    }

    @Override
    @DoNotChange
    public Iterable<ImmutableMetaDocPartIndexColumn> getAddedMetaDocPartIndexColumns() {
        return addedColumns;
    }

    @Override
    public ImmutableMetaDocPartIndex immutableCopy() {
        if (addedColumns.isEmpty()) {
            return wrapped;
        }
        else {
            ImmutableMetaDocPartIndex.Builder builder = new Builder(wrapped);
            for (ImmutableMetaDocPartIndexColumn addedField : addedColumns) {
                builder.add(addedField);
            }
            return builder.build();
        }
    }

    @Override
    public String getIdentifier() {
        return wrapped.getIdentifier();
    }

    @Override
    public boolean isUnique() {
        return wrapped.isUnique();
    }
    
    @Override
    public int size() {
        return newColumns.size();
    }

    @Override
    public Iterator<? extends ImmutableMetaDocPartIndexColumn> iteratorColumns() {
        return Seq.seq(wrapped.iteratorColumns())
                .concat(addedColumns.stream())
                .iterator();
    }

    @Override
    public ImmutableMetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName) {
        return newColumns.get(columnName);
    }

    @Override
    public MetaDocPartIndexColumn getMetaDocPartIndexColumnByPosition(int position) {
        if (position < wrapped.size()) {
            return wrapped.getMetaDocPartIndexColumnByPosition(position);
        }
        return addedColumns.get(position - wrapped.size());
    }

    @Override
    public boolean hasSameColumns(MetaDocPartIndex docPartIndex) {
        return wrapped.hasSameColumns(this, iteratorColumns());
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
