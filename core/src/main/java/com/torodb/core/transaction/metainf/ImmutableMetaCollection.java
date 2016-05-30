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

import com.google.common.base.Preconditions;
import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class ImmutableMetaCollection implements MetaCollection<ImmutableMetaDocPart> {

    private final String name;
    private final String identifier;
    private final Map<TableRef, ImmutableMetaDocPart> docPartsByTableRef;
    private final Map<String, ImmutableMetaDocPart> docPartsByIdentifier;

    public ImmutableMetaCollection(String colName, String colId, Iterable<ImmutableMetaDocPart> docParts) {
        this.name = colName;
        this.identifier = colId;

        HashMap<TableRef, ImmutableMetaDocPart> byTableRef = new HashMap<>();
        HashMap<String, ImmutableMetaDocPart> byDbName = new HashMap<>();
        for (ImmutableMetaDocPart table : docParts) {
            byTableRef.put(table.getTableRef(), table);
            byDbName.put(table.getIdentifier(), table);
        }
        this.docPartsByTableRef = Collections.unmodifiableMap(byTableRef);
        this.docPartsByIdentifier = Collections.unmodifiableMap(byDbName);
    }

    public ImmutableMetaCollection(String docName, String dbName, @DoNotChange Map<String, ImmutableMetaDocPart> docPartsById) {
        this.name = docName;
        this.identifier = dbName;

        this.docPartsByIdentifier = docPartsById;
        this.docPartsByTableRef = new HashMap<>();
        for (ImmutableMetaDocPart table : docPartsById.values()) {
            docPartsByTableRef.put(table.getTableRef(), table);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Stream<ImmutableMetaDocPart> streamContainedMetaDocParts() {
        return docPartsByIdentifier.values().stream();
    }

    @Override
    public ImmutableMetaDocPart getMetaDocPartByIdentifier(String tableDbName) {
        return docPartsByIdentifier.get(tableDbName);
    }

    @Override
    public ImmutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef) {
        return docPartsByTableRef.get(tableRef);
    }
   
    public static class Builder {
        private boolean built = false;
        private final String name;
        private final String identifier;
        private final Map<String, ImmutableMetaDocPart> docPartsByDbName;

        public Builder(String name, String identifier) {
            this.name = name;
            this.identifier = identifier;
            this.docPartsByDbName = new HashMap<>();
        }

        public Builder(String name, String identifier, int expectedDocParts) {
            this.name = name;
            this.identifier = identifier;
            this.docPartsByDbName = new HashMap<>(expectedDocParts);
        }

        public Builder add(ImmutableMetaDocPart.Builder tableBuilder) {
            return add(tableBuilder.build());
        }

        public Builder add(ImmutableMetaDocPart table) {
            Preconditions.checkState(!built, "This builder has already been built");
            docPartsByDbName.put(table.getIdentifier(), table);
            return this;
        }

        public ImmutableMetaCollection build() {
            Preconditions.checkState(!built, "This builder has already been built");
            built = true;
            return new ImmutableMetaCollection(name, identifier, docPartsByDbName);
        }
    }
}
