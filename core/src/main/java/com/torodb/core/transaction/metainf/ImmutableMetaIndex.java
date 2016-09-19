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

package com.torodb.core.transaction.metainf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.torodb.core.TableRef;

/**
 *
 */
public class ImmutableMetaIndex implements MetaIndex {

    private final String name;
    private final boolean unique;
    private final List<ImmutableMetaIndexField> fieldsByPosition;
    private final Table<TableRef, String, ImmutableMetaIndexField> fieldsByTableRefAndName;

    public ImmutableMetaIndex(String name, boolean unique) {
        this(name, unique, Collections.emptyList());
    }

    public ImmutableMetaIndex(String name, boolean unique, Iterable<ImmutableMetaIndexField> fields) {
        this.name = name;
        this.unique = unique;

        fieldsByTableRefAndName = HashBasedTable.create();
        fieldsByPosition = new ArrayList<>(fieldsByTableRefAndName.size());

        for (ImmutableMetaIndexField field : fields) {
            fieldsByTableRefAndName.put(field.getTableRef(), field.getName(), field);
            fieldsByPosition.add(field);
        }
    }

    public ImmutableMetaIndex(String name, boolean unique, List<ImmutableMetaIndexField> fieldsByPosition) {
        this.name = name;
        this.unique = unique;
        this.fieldsByPosition = fieldsByPosition;
        this.fieldsByTableRefAndName = HashBasedTable.create();
        for (ImmutableMetaIndexField field : fieldsByPosition) {
            fieldsByTableRefAndName.put(field.getTableRef(), field.getName(), field);
        }
    }

    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public int size() {
        return fieldsByPosition.size();
    }

    @Override
    public Stream<ImmutableMetaIndexField> streamFields() {
        return fieldsByPosition.stream();
    }

    @Override
    public Stream<? extends ImmutableMetaIndexField> streamMetaIndexFieldByTableRef(TableRef tableRef) {
        return fieldsByTableRefAndName.row(tableRef).values().stream();
    }

    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name) {
        return fieldsByTableRefAndName.get(tableRef, name);
    }
    
    @Override
    public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position) {
        return fieldsByPosition.get(position);
    }

    @Override
    public String toString() {
        return defautToString();
    }

    public static class Builder {
        private boolean built = false;
        private final String name;
        private final boolean unique;
        private final List<ImmutableMetaIndexField> fieldsByPosition;

        public Builder(String name, boolean unique, String identifier) {
            this.name = name;
            this.unique = unique;
            fieldsByPosition = new ArrayList<>();
        }

        public Builder(String name, boolean unique, int expectedFields) {
            this.name = name;
            this.unique = unique;
            fieldsByPosition = new ArrayList<>(expectedFields);
        }

        public Builder(ImmutableMetaIndex other) {
            this.name = other.name;
            this.unique = other.isUnique();
            fieldsByPosition = new ArrayList<>(other.fieldsByPosition);
        }

        public Builder add(ImmutableMetaIndexField field) {
            Preconditions.checkState(!built, "This builder has already been built");
            fieldsByPosition.add(field);
            return this;
        }

        public Builder remove(MetaIndexField field) {
            Preconditions.checkState(!built, "This builder has already been built");
            fieldsByPosition.remove(field.getPosition());
            return this;
        }

        public ImmutableMetaIndex build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaIndex(name, unique, fieldsByPosition);
        }
    }

}
