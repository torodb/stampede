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

package com.torodb.metainfo.cache;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import com.torodb.kvdocument.types.KVType;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDocPart implements MutableMetaDocPart {

    private final TableRef tableRef;
    private final String identifier;
    private final Table<String, KVType, MetaField> newFields;

    public WrapperMutableMetaDocPart(MetaDocPart<ImmutableMetaField> originaLDocPart) {
        this.tableRef = originaLDocPart.getTableRef();
        this.identifier = originaLDocPart.getIdentifier();

        newFields = HashBasedTable.create();

        originaLDocPart.streamFields().forEach((field) ->
            newFields.put(field.getName(), field.getType(), field)
        );
    }

    @Override
    public ImmutableMetaField addMetaField(String name, String identifier, KVType type) throws
            IllegalArgumentException {
        if (getMetaFieldByNameAndType(name, type) != null) {
            throw new IllegalArgumentException("There is another column with the name " + name +
                    " whose type is " + type);
        }

        assert getMetaFieldByIdentifier(identifier) != null : "There is another column with the identifier " + identifier;

        ImmutableMetaField newField = new ImmutableMetaField(name, identifier, type);
        newFields.put(name, type, newField);
        return newField;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Stream<MetaField> streamFields() {
        return newFields.values().stream();
    }

    @Override
    public Stream<MetaField> streamMetaFieldByName(String columnName) {
        return newFields.row(columnName).values().stream();
    }

    @Override
    public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, KVType type) {
        return (ImmutableMetaField) newFields.get(fieldName, type);
    }

    @Override
    public ImmutableMetaField getMetaFieldByIdentifier(String fieldId) {
        return (ImmutableMetaField) newFields.values().stream()
                .filter((field) -> field.getIdentifier().equals(fieldId))
                .findAny()
                .orElse(null);
    }

}
