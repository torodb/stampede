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
    private final Table<String, FieldType, ImmutableMetaFieldIndex> newFields;
    /**
     * This list just contains the fields that have been added on this wrapper but not on the
     * wrapped object.
     */
    private final List<ImmutableMetaFieldIndex> addedFields;
    private final Consumer<WrapperMutableMetaDocPartIndex> changeConsumer;

    public WrapperMutableMetaDocPartIndex(ImmutableMetaDocPartIndex wrapped,
            Consumer<WrapperMutableMetaDocPartIndex> changeConsumer) {
        this.wrapped = wrapped;
        
        newFields = HashBasedTable.create();

        wrapped.streamFields().forEach((field) ->
            newFields.put(field.getName(), field.getType(), field)
        );
        addedFields = new ArrayList<>();
        this.changeConsumer = changeConsumer;
    }

    @Override
    public ImmutableMetaFieldIndex addMetaFieldIndex(String name, String identifier, FieldType type, FieldIndexOrdering ordering) throws
            IllegalArgumentException {
        if (getMetaFieldIndexByNameAndType(name, type) != null) {
            throw new IllegalArgumentException("There is another field with the name " + name +
                    " whose type is " + type);
        }

        ImmutableMetaFieldIndex newField = new ImmutableMetaFieldIndex(size(),name, type, ordering);
        newFields.put(name, type, newField);
        addedFields.add(newField);
        changeConsumer.accept(this);
        return newField;
    }

    @Override
    @DoNotChange
    public Iterable<ImmutableMetaFieldIndex> getAddedMetaFieldIndexes() {
        return addedFields;
    }

    @Override
    public ImmutableMetaDocPartIndex immutableCopy() {
        if (addedFields.isEmpty()) {
            return wrapped;
        }
        else {
            ImmutableMetaDocPartIndex.Builder builder = new Builder(wrapped);
            for (ImmutableMetaFieldIndex addedField : addedFields) {
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
        return newFields.size();
    }

    @Override
    public Stream<? extends ImmutableMetaFieldIndex> streamFields() {
        return newFields.values().stream();
    }

    @Override
    public ImmutableMetaFieldIndex getMetaFieldIndexByNameAndType(String fieldName, FieldType type) {
        return newFields.get(fieldName, type);
    }

    @Override
    public MetaFieldIndex getMetaFieldIndexByPosition(int position) {
        if (position < wrapped.size()) {
            return wrapped.getMetaFieldIndexByPosition(position);
        }
        return addedFields.get(position - wrapped.size());
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
