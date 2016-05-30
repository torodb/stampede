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

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaCollection implements MutableMetaCollection<MutableMetaDocPart> {
    
    private final String name;
    private final String identifier;
    private final HashMap<TableRef, MutableMetaDocPart> newDocParts;

    public WrapperMutableMetaCollection(MetaCollection<? extends MetaDocPart> originalCollection) {
        this.name = originalCollection.getName();
        this.identifier = originalCollection.getIdentifier();
        
        this.newDocParts = new HashMap<>();
        originalCollection.streamContainedMetaDocParts().forEach((docPart) -> {
            @SuppressWarnings("unchecked")
            MutableMetaDocPart mutable = new WrapperMutableMetaDocPart(docPart);
            newDocParts.put(mutable.getTableRef(), mutable);
        });
    }

    @Override
    public MutableMetaDocPart addMetaDocPart(TableRef tableRef, String tableId) throws
            IllegalArgumentException {
        if (getMetaDocPartByTableRef(tableRef) != null) {
            throw new IllegalArgumentException("There is another doc part whose table ref is " + tableRef);
        }

        assert getMetaDocPartByIdentifier(tableId) == null : "There is another doc part whose id is " + tableRef;

        MutableMetaDocPart result = new WrapperMutableMetaDocPart(
                new ImmutableMetaDocPart(tableRef, tableId, Collections.emptyMap())
        );

        newDocParts.put(tableRef, result);

        return result;
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
    public Stream<MutableMetaDocPart> streamContainedMetaDocParts() {
        return newDocParts.values().stream();
    }

    @Override
    public MutableMetaDocPart getMetaDocPartByIdentifier(String docPartId) {
        Optional<MutableMetaDocPart> newDocPart = newDocParts.values().stream()
                .filter((docPart) -> docPart.getIdentifier().equals(docPartId))
                .findAny();

        return newDocPart.orElse(null);
    }

    @Override
    public MutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef) {
        return newDocParts.get(tableRef);
    }

}
