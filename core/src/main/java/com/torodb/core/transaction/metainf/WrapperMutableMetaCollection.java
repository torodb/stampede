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

import com.torodb.core.TableRef;
import com.torodb.core.annotations.DoNotChange;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaCollection implements MutableMetaCollection {

    private final ImmutableMetaCollection wrapped;
    private final HashMap<TableRef, WrapperMutableMetaDocPart> newDocParts;
    private final Set<WrapperMutableMetaDocPart> modifiedMetaDocParts;
    private final HashMap<String, WrapperMutableMetaIndex> newIndexes;
    private final Set<WrapperMutableMetaIndex> modifiedMetaIndexes;
    private final Consumer<WrapperMutableMetaCollection> changeConsumer;

    public WrapperMutableMetaCollection(ImmutableMetaCollection wrappedCollection,
            Consumer<WrapperMutableMetaCollection> changeConsumer) {
        this.wrapped = wrappedCollection;
        this.changeConsumer = changeConsumer;
        
        this.newDocParts = new HashMap<>();

        modifiedMetaDocParts = new HashSet<>();

        wrappedCollection.streamContainedMetaDocParts().forEach((docPart) -> {
            WrapperMutableMetaDocPart mutable = createMetaDocPart(docPart);
            newDocParts.put(mutable.getTableRef(), mutable);
        });
        
        this.newIndexes = new HashMap<>();

        modifiedMetaIndexes = new HashSet<>();

        wrappedCollection.streamContainedMetaIndexes().forEach((index) -> {
            WrapperMutableMetaIndex mutable = createMetaIndex(index);
            newIndexes.put(mutable.getName(), mutable);
        });
    }

    protected WrapperMutableMetaDocPart createMetaDocPart(ImmutableMetaDocPart immutable) {
        return new WrapperMutableMetaDocPart(immutable, this::onDocPartChange);
    }

    protected WrapperMutableMetaIndex createMetaIndex(ImmutableMetaIndex immutable) {
        return new WrapperMutableMetaIndex(immutable, this::onIndexChange);
    }

    @Override
    public WrapperMutableMetaDocPart addMetaDocPart(TableRef tableRef, String tableId) throws
            IllegalArgumentException {
        if (getMetaDocPartByTableRef(tableRef) != null) {
            throw new IllegalArgumentException("There is another doc part whose table ref is " + tableRef);
        }

        assert getMetaDocPartByIdentifier(tableId) == null : "There is another doc part whose id is " + tableRef;

        WrapperMutableMetaDocPart result = createMetaDocPart(
                new ImmutableMetaDocPart(tableRef, tableId));

        newDocParts.put(tableRef, result);
        onDocPartChange(result);

        return result;
    }

    @Override
    @DoNotChange
    public Iterable<? extends WrapperMutableMetaDocPart> getModifiedMetaDocParts() {
        return modifiedMetaDocParts;
    }

    @Override
    public MutableMetaIndex addMetaIndex(String name, boolean unique) throws IllegalArgumentException {
        if (getMetaIndexByName(name) != null) {
            throw new IllegalArgumentException("There is another index whose name is " + name);
        }

        WrapperMutableMetaIndex result = createMetaIndex(
                new ImmutableMetaIndex(name, unique));

        newIndexes.put(name, result);
        onIndexChange(result);

        return result;
    }

    @Override
    public Iterable<? extends MutableMetaIndex> getModifiedMetaIndexes() {
        return modifiedMetaIndexes;
    }

    @Override
    public ImmutableMetaCollection immutableCopy() {
        if (modifiedMetaDocParts.isEmpty() && modifiedMetaIndexes.isEmpty()) {
            return wrapped;
        } else {
            ImmutableMetaCollection.Builder builder = new ImmutableMetaCollection.Builder(wrapped);
            for (MutableMetaDocPart modifiedMetaDocPart : modifiedMetaDocParts) {
                builder.put(modifiedMetaDocPart.immutableCopy());
            }
            for (MutableMetaIndex modifiedMetaIndex : modifiedMetaIndexes) {
                builder.put(modifiedMetaIndex.immutableCopy());
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
    public Stream<? extends WrapperMutableMetaDocPart> streamContainedMetaDocParts() {
        return newDocParts.values().stream();
    }

    @Override
    public WrapperMutableMetaDocPart getMetaDocPartByIdentifier(String docPartId) {
        Optional<WrapperMutableMetaDocPart> newDocPart = newDocParts.values().stream()
                .filter((docPart) -> docPart.getIdentifier().equals(docPartId))
                .findAny();

        return newDocPart.orElse(null);
    }

    @Override
    public WrapperMutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef) {
        return newDocParts.get(tableRef);
    }

    @Override
    public Stream<? extends WrapperMutableMetaIndex> streamContainedMetaIndexes() {
        return newIndexes.values().stream();
    }

    @Override
    public WrapperMutableMetaIndex getMetaIndexByName(String indexName) {
        return newIndexes.get(indexName);
    }

    @Override
    public String toString() {
        return defautToString();
    }

    protected void onDocPartChange(WrapperMutableMetaDocPart changedDocPart) {
        modifiedMetaDocParts.add(changedDocPart);
        changeConsumer.accept(this);
    }

    protected void onIndexChange(WrapperMutableMetaIndex changedIndex) {
        modifiedMetaIndexes.add(changedIndex);
        changeConsumer.accept(this);
    }

}
