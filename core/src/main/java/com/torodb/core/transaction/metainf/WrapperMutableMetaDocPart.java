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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart.Builder;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDocPart implements MutableMetaDocPart {

    private final ImmutableMetaDocPart wrapped;
    /**
     * This table contains all fields contained by wrapper and all new fields
     */
    private final Table<String, FieldType, ImmutableMetaField> newFields;
    /**
     * This list just contains the fields that have been added on this wrapper but not on the
     * wrapped object.
     */
    private final List<ImmutableMetaField> addedFields;
    private final Consumer<WrapperMutableMetaDocPart> changeConsumer;
    private final EnumMap<FieldType, ImmutableMetaScalar> newScalars;
    private final Map<String, WrapperMutableMetaDocPartIndex> newIndexes;

    public WrapperMutableMetaDocPart(ImmutableMetaDocPart wrapped,
            Consumer<WrapperMutableMetaDocPart> changeConsumer) {
        this.wrapped = wrapped;
        
        newFields = HashBasedTable.create();

        wrapped.streamFields().forEach((field) ->
            newFields.put(field.getName(), field.getType(), field)
        );
        addedFields = new ArrayList<>();
        this.changeConsumer = changeConsumer;
        this.newScalars = new EnumMap<>(FieldType.class);
        newIndexes = new HashMap<>();
    }

    @Override
    public ImmutableMetaField addMetaField(String name, String identifier, FieldType type) throws
            IllegalArgumentException {
        if (getMetaFieldByNameAndType(name, type) != null) {
            throw new IllegalArgumentException("There is another field with the name " + name +
                    " whose type is " + type);
        }

        assert getMetaFieldByIdentifier(identifier) == null : "There is another field with the identifier " + identifier;

        ImmutableMetaField newField = new ImmutableMetaField(name, identifier, type);
        newFields.put(name, type, newField);
        addedFields.add(newField);
        changeConsumer.accept(this);
        return newField;
    }

    @Override
    public ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws
            IllegalArgumentException {
        if (getScalar(type) != null) {
            throw new IllegalArgumentException("There is another scalar with type " + type + ", "
                    + "whose identifier is " + identifier);
        }
        ImmutableMetaScalar scalar = new ImmutableMetaScalar(identifier, type);
        newScalars.put(type, scalar);
        changeConsumer.accept(this);
        return scalar;
    }

    @Override
    @DoNotChange
    public Iterable<ImmutableMetaField> getAddedMetaFields() {
        return addedFields;
    }

    @Override
    public Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars() {
        return newScalars.values();
    }

    @Override
    public MutableMetaDocPartIndex addMetaDocPartIndex(String identifier, boolean unique) throws
            IllegalArgumentException {
        if (getMetaDocPartIndexByIdentifier(identifier) != null) {
            throw new IllegalArgumentException("There is another index with the identifier " + identifier);
        }

        WrapperMutableMetaDocPartIndex newIndex = new WrapperMutableMetaDocPartIndex(
                new ImmutableMetaDocPartIndex(identifier, unique), index -> {});
        newIndexes.put(identifier, newIndex);
        changeConsumer.accept(this);
        return newIndex;
    }

    @Override
    @DoNotChange
    public Iterable<? extends WrapperMutableMetaDocPartIndex> getAddedMetaDocPartIndexes() {
        return newIndexes.values();
    }

    @Override
    public ImmutableMetaDocPart immutableCopy() {
        if (addedFields.isEmpty() && newScalars.isEmpty()) {
            return wrapped;
        }
        else {
            ImmutableMetaDocPart.Builder builder = new Builder(wrapped);
            for (ImmutableMetaField addedField : addedFields) {
                builder.put(addedField);
            }
            for (ImmutableMetaScalar value : newScalars.values()) {
                builder.put(value);
            }
            return builder.build();
        }
    }

    @Override
    public TableRef getTableRef() {
        return wrapped.getTableRef();
    }

    @Override
    public String getIdentifier() {
        return wrapped.getIdentifier();
    }

    @Override
    public Stream<? extends ImmutableMetaField> streamFields() {
        return newFields.values().stream();
    }

    @Override
    public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String columnName) {
        return newFields.row(columnName).values().stream();
    }

    @Override
    public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type) {
        return newFields.get(fieldName, type);
    }

    @Override
    public ImmutableMetaField getMetaFieldByIdentifier(String fieldId) {
        return newFields.values().stream()
                .filter((field) -> field.getIdentifier().equals(fieldId))
                .findAny()
                .orElse(null);
    }

    @Override
    public Stream<? extends MetaScalar> streamScalars() {
        return Stream.concat(newScalars.values().stream(), wrapped.streamScalars());
    }

    @Override
    public MetaScalar getScalar(FieldType type) {
        ImmutableMetaScalar scalar = newScalars.get(type);
        if (scalar != null) {
            return scalar;
        }
        return wrapped.getScalar(type);
    }

    @Override
    public Stream<? extends MetaDocPartIndex> streamIndexes() {
        return Stream.concat(newIndexes.values().stream(), wrapped.streamIndexes());
    }

    @Override
    public MetaDocPartIndex getMetaDocPartIndexByIdentifier(String indexId) {
        MetaDocPartIndex index = wrapped.getMetaDocPartIndexByIdentifier(indexId);
        
        if (index != null) {
            return index;
        }
        
        return newIndexes.get(indexId);
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
