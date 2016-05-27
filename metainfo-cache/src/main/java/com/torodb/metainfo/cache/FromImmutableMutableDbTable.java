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
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.kvdocument.types.KVType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MutableDocPart;

/**
 *
 */
public class FromImmutableMutableDbTable extends MutableDocPart {

    private final ImmutableMetaDocPart originalTable;
    private final Table<String, KVType, ImmutableMetaField> newColumns = HashBasedTable.create();

    public FromImmutableMutableDbTable(ImmutableMetaDocPart originalTable) {
        this.originalTable = originalTable;
    }

    @Override
    public void addColumn(String docName, String dbName, KVType type) throws IllegalArgumentException {
        if (getColumnByDbName(dbName) != null) {
            throw new IllegalArgumentException("There is another column with the db name " + dbName);
        }
        if (getColumnByDocNameAndType(docName, type) != null) {
            throw new IllegalArgumentException("There is another column with the doc name "
                    + docName + " whose type is " + type);
        }

        newColumns.put(docName, type, new ImmutableMetaField(docName, dbName, type));
    }

    @Override
    public TableRef getTableRef() {
        return originalTable.getTableRef();
    }

    @Override
    public String getDbName() {
        return originalTable.getIdentifier();
    }

    @Override
    public Iterable<? extends MetaField> getColumns() {
        Map<String, MetaField> allColumnsCopy = new HashMap<>(originalTable.countColumns());
        
        Iterable<ImmutableMetaField> originalColumns = originalTable.getColumns();
        
        originalColumns.forEach((column) -> allColumnsCopy.put(column.getIdentifier(), column));
        
        newColumns.values().forEach((column) -> allColumnsCopy.put(column.getIdentifier(), column));

        return allColumnsCopy.values();
    }

    @Override
    public MetaField getColumnByDbName(String columnDbName) {
        Optional<ImmutableMetaField> newColumn = newColumns.values().stream()
                .filter((column) -> column.getIdentifier().equals(columnDbName))
                .findAny();
        if (!newColumn.isPresent()) {
            return originalTable.getMetaFieldByIdentifier(columnDbName);
        }
        return newColumn.get();
    }

    @Override
    public Stream<? extends MetaField> streamColumnsByDocName(String columnDocName) {
        Stream<ImmutableMetaField> newColumnsStream
                = newColumns.row(columnDocName).values().stream();
        Stream<ImmutableMetaField> oldColumnsStream
                = originalTable.streamMetaFieldByName(columnDocName);
        return Stream.concat(oldColumnsStream, newColumnsStream);
    }

    @Override
    public MetaField getColumnByDocNameAndType(String columnDocName, KVType type) {
        MetaField column = newColumns.get(columnDocName, type);
        if (column == null) {
            column = originalTable.getMetaFieldByNameAndType(columnDocName, type);
        }
        return column;
    }

}
