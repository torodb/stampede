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

import com.torodb.core.transaction.metainf.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDatabase implements MutableMetaDatabase<MutableMetaCollection> {

    private final String name;
    private final String identifier;
    private final Map<String, MutableMetaCollection> newCollections;

    public WrapperMutableMetaDatabase(MetaDatabase<? extends MetaCollection> originalDatabase) {
        this.name = originalDatabase.getName();
        this.identifier = originalDatabase.getIdentifier();

        this.newCollections = new HashMap<>();
        originalDatabase.streamMetaCollections().forEach((collection) -> {
            @SuppressWarnings("unchecked")
            MutableMetaCollection mutable = new WrapperMutableMetaCollection(collection);
                    
            newCollections.put(collection.getName(), mutable);
        });
    }

    @Override
    public MutableMetaCollection addMetaCollection(String colName, String colId) throws
            IllegalArgumentException {
        if (getMetaCollectionByName(colName) != null) {
            throw new IllegalArgumentException("There is another collection whose name is " + colName);
        }

        assert getMetaCollectionByIdentifier(colId) == null : "There is another collection whose id is " + colId;

        MutableMetaCollection result = new WrapperMutableMetaCollection(
                new ImmutableMetaCollection(colName, identifier, Collections.emptyMap())
        );

        newCollections.put(colName, result);

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
    public Stream<MutableMetaCollection> streamMetaCollections() {
        return newCollections.values().stream();
    }

    @Override
    public MutableMetaCollection getMetaCollectionByName(String collectionName) {
        return newCollections.get(collectionName);
    }

    @Override
    public MutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier) {
        return newCollections.values().stream()
                .filter((collection) -> collection.getIdentifier().equals(collectionIdentifier))
                .findAny()
                .orElse(null);
    }

}
