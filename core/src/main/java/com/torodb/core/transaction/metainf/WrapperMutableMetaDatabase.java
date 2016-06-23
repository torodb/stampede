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

import com.torodb.core.annotations.DoNotChange;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaDatabase implements MutableMetaDatabase {

    private final ImmutableMetaDatabase wrapped;
    private final Map<String, WrapperMutableMetaCollection> newCollections;
    private final Set<WrapperMutableMetaCollection> modifiedMetaCollections;
    private final Consumer<WrapperMutableMetaDatabase> changeConsumer;

    public WrapperMutableMetaDatabase(ImmutableMetaDatabase wrapped,
            Consumer<WrapperMutableMetaDatabase> changeConsumer) {
        this.wrapped = wrapped;
        this.changeConsumer = changeConsumer;

        this.newCollections = new HashMap<>();
        this.modifiedMetaCollections = new HashSet<>();
        
        wrapped.streamMetaCollections().forEach((collection) -> {
            WrapperMutableMetaCollection mutable = createMetaColletion(collection);
                    
            newCollections.put(collection.getName(), mutable);
        });
    }

    protected WrapperMutableMetaCollection createMetaColletion(ImmutableMetaCollection immutable) {
        return new WrapperMutableMetaCollection(immutable, this::onMetaCollectionChange);
    }

    @Override
    public WrapperMutableMetaCollection addMetaCollection(String colName, String colId) throws
            IllegalArgumentException {
        if (getMetaCollectionByName(colName) != null) {
            throw new IllegalArgumentException("There is another collection whose name is " + colName);
        }

        assert getMetaCollectionByIdentifier(colId) == null : "There is another collection whose id is " + colId;

        WrapperMutableMetaCollection result = createMetaColletion(
                new ImmutableMetaCollection(colName, colId, Collections.emptyMap()));

        newCollections.put(colName, result);
        onMetaCollectionChange(result);

        return result;
    }

    @DoNotChange
    @Override
    public Iterable<? extends WrapperMutableMetaCollection> getModifiedCollections() {
        return modifiedMetaCollections;
    }

    @Override
    public ImmutableMetaDatabase immutableCopy() {
        if (modifiedMetaCollections.isEmpty()) {
            return wrapped;
        } else {
            ImmutableMetaDatabase.Builder builder = new ImmutableMetaDatabase.Builder(wrapped);
            for (MutableMetaCollection modifiedMetaCollection : modifiedMetaCollections) {
                builder.add(modifiedMetaCollection.immutableCopy());
            }
            return builder.build();
        }
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public String getIdentifier() {
        return wrapped.getIdentifier();
    }

    @Override
    public Stream<? extends WrapperMutableMetaCollection> streamMetaCollections() {
        return newCollections.values().stream();
    }

    @Override
    public WrapperMutableMetaCollection getMetaCollectionByName(String collectionName) {
        return newCollections.get(collectionName);
    }

    @Override
    public WrapperMutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier) {
        return newCollections.values().stream()
                .filter((collection) -> collection.getIdentifier().equals(collectionIdentifier))
                .findAny()
                .orElse(null);
    }

    private void onMetaCollectionChange(WrapperMutableMetaCollection changed) {
        modifiedMetaCollections.add(changed);
        changeConsumer.accept(this);
    }

}
