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
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDocPart implements MutableMetaDocPart {

    private final ImmutableMetaDocPart wrapped;
    private final Table<String, FieldType, ImmutableMetaField> newFields;
    private final List<ImmutableMetaField> addedFields;
    private final Consumer<WrapperMutableMetaDocPart> changeConsumer;

    public WrapperMutableMetaDocPart(ImmutableMetaDocPart wrapped,
            Consumer<WrapperMutableMetaDocPart> changeConsumer) {
        this.wrapped = wrapped;
        
        newFields = HashBasedTable.create();

        wrapped.streamFields().forEach((field) ->
            newFields.put(field.getName(), field.getType(), field)
        );
        addedFields = new ArrayList<>();
        this.changeConsumer = changeConsumer;
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
    @DoNotChange
    public Iterable<ImmutableMetaField> getAddedMetaFields() {
        return addedFields;
    }

    @Override
    public ImmutableMetaDocPart immutableCopy() {
        if (addedFields.isEmpty()) {
            return wrapped;
        }
        else {
            ImmutableMetaDocPart.Builder builder = new Builder(wrapped);
            for (ImmutableMetaField addedField : addedFields) {
                builder.add(addedField);
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

}
