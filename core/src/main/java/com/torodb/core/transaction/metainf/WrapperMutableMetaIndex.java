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
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaIndex.Builder;

/**
 *
 */
public class WrapperMutableMetaIndex implements MutableMetaIndex {

    private final ImmutableMetaIndex wrapped;
    /**
     * This table contains all fields contained by wrapper and all new fields
     */
    private final Table<TableRef, String, ImmutableMetaIndexField> newFields;
    /**
     * This list just contains the fields that have been added on this wrapper but not on the
     * wrapped object.
     */
    private final List<ImmutableMetaIndexField> addedFields;
    private final Consumer<WrapperMutableMetaIndex> changeConsumer;

    public WrapperMutableMetaIndex(ImmutableMetaIndex wrapped,
            Consumer<WrapperMutableMetaIndex> changeConsumer) {
        this.wrapped = wrapped;
        
        newFields = HashBasedTable.create();

        wrapped.streamFields().forEach((field) ->
            newFields.put(field.getTableRef(), field.getName(), field)
        );
        addedFields = new ArrayList<>();
        this.changeConsumer = changeConsumer;
    }

    @Override
    public ImmutableMetaIndexField addMetaIndexField(TableRef tableRef, String name, FieldIndexOrdering ordering) throws
            IllegalArgumentException {
        if (getMetaIndexFieldByTableRefAndName(tableRef, name) != null) {
            throw new IllegalArgumentException("There is another field with tableRef " + tableRef + " whose name is " + name);
        }

        ImmutableMetaIndexField newField = new ImmutableMetaIndexField(size(), tableRef, name, ordering);
        newFields.put(tableRef, name, newField);
        addedFields.add(newField);
        changeConsumer.accept(this);
        return newField;
    }

    @Override
    @DoNotChange
    public Iterable<ImmutableMetaIndexField> getAddedMetaIndexFields() {
        return addedFields;
    }

    @Override
    public ImmutableMetaIndex immutableCopy() {
        if (addedFields.isEmpty()) {
            return wrapped;
        }
        else {
            ImmutableMetaIndex.Builder builder = new Builder(wrapped);
            for (ImmutableMetaIndexField addedField : addedFields) {
                builder.add(addedField);
            }
            return builder.build();
        }
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }
    
    @Override
    public boolean isUnique() {
        return wrapped.isUnique();
    }

    @Override
    public int size() {
        return newFields.size();
    }

    @Override
    public Stream<? extends ImmutableMetaIndexField> streamFields() {
        return newFields.values().stream();
    }

    @Override
    public Stream<? extends ImmutableMetaIndexField> streamMetaIndexFieldByTableRef(TableRef tableRef) {
        return newFields.row(tableRef).values().stream();
    }

    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position) {
        if (position < wrapped.size()) {
            return wrapped.getMetaIndexFieldByPosition(position);
        }
        return addedFields.get(position - wrapped.size());
    }

    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String fieldName) {
        return newFields.get(tableRef, fieldName);
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
