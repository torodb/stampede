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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.transaction.metainf.ImmutableMetaDocPartIndex.Builder;

/**
 *
 */
public class WrapperMutableMetaDocPartIndex extends AbstractMetaDocPartIndex implements MutableMetaDocPartIndex {

    /**
     * This table contains all fields contained by wrapper and all new fields
     */
    private final Map<String, ImmutableMetaDocPartIndexColumn> addedColumnsByIdentifier;
    /**
     * This list just contains the fields that have been added on this wrapper but not on the
     * wrapped object.
     */
    private final List<ImmutableMetaDocPartIndexColumn> addedColumns;
    private final Consumer<WrapperMutableMetaDocPartIndex> changeConsumer;
    
    private String identifier;
    private boolean mutable = true;

    public WrapperMutableMetaDocPartIndex(ImmutableMetaDocPartIndex docPartIndex, Consumer<WrapperMutableMetaDocPartIndex> changeConsumer) {
        this(docPartIndex.getIdentifier(), docPartIndex.isUnique(), changeConsumer);
        
        makeImmutable(docPartIndex.getIdentifier());
    }

    public WrapperMutableMetaDocPartIndex(boolean unique, Consumer<WrapperMutableMetaDocPartIndex> changeConsumer) {
        this(null, unique, changeConsumer);
    }

    private WrapperMutableMetaDocPartIndex(String identifier, boolean unique, Consumer<WrapperMutableMetaDocPartIndex> changeConsumer) {
        super(identifier, unique);
        addedColumnsByIdentifier = new HashMap<>();
        addedColumns = new ArrayList<>();
        this.changeConsumer = changeConsumer;
    }

    @Override
    public String getIdentifier() {
        Preconditions.checkArgument(!mutable, "Must be immutable to be read");
        return identifier;
    }

    @Override
    public ImmutableMetaDocPartIndexColumn putMetaDocPartIndexColumn(int position, String identifier, FieldIndexOrdering ordering) throws
            IllegalArgumentException {
        Preconditions.checkArgument(this.mutable, "This object has been marked as immutable");
        if (getMetaDocPartIndexColumnByIdentifier(identifier) != null) {
            throw new IllegalArgumentException("There is another column with the identifier " + identifier);
        }
        if (getMetaDocPartIndexColumnByPosition(position) != null) {
            throw new IllegalArgumentException("There is another column with the position " + position);
        }

        ImmutableMetaDocPartIndexColumn newField = new ImmutableMetaDocPartIndexColumn(position, identifier, ordering);
        addedColumnsByIdentifier.put(identifier, newField);
        if (addedColumns.size() <= position) {
            IntStream.range(addedColumns.size(), position + 1)
                .forEach(index -> addedColumns.add(null));
        }
        addedColumns.set(position, newField);
        return newField;
    }

    @Override
    public ImmutableMetaDocPartIndexColumn addMetaDocPartIndexColumn(String identifier, FieldIndexOrdering ordering) throws
            IllegalArgumentException {
        if (getMetaDocPartIndexColumnByIdentifier(identifier) != null) {
            throw new IllegalArgumentException("There is another column with the identifier " + identifier);
        }

        ImmutableMetaDocPartIndexColumn newField = new ImmutableMetaDocPartIndexColumn(size(), identifier, ordering);
        addedColumnsByIdentifier.put(identifier, newField);
        addedColumns.add(newField);
        return newField;
    }

    @Override
    @DoNotChange
    public Iterable<ImmutableMetaDocPartIndexColumn> getAddedMetaDocPartIndexColumns() {
        return addedColumns;
    }

    @Override
    public ImmutableMetaDocPartIndex immutableCopy() {
        Preconditions.checkArgument(!mutable, "Must be marked immutable");
        ImmutableMetaDocPartIndex.Builder builder = new Builder(this);
        for (ImmutableMetaDocPartIndexColumn addedField : addedColumns) {
            builder.add(addedField);
        }
        return builder.build();
    }

    @Override
    public int size() {
        return addedColumnsByIdentifier.size();
    }

    @Override
    public Iterator<? extends ImmutableMetaDocPartIndexColumn> iteratorColumns() {
        return addedColumns.iterator();
    }

    @Override
    public ImmutableMetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName) {
        return addedColumnsByIdentifier.get(columnName);
    }

    @Override
    public MetaDocPartIndexColumn getMetaDocPartIndexColumnByPosition(int position) {
        if (position >= addedColumns.size()) {
            return null;
        }
        return addedColumns.get(position);
    }

    @Override
    public boolean isMutable() {
        return mutable;
    }

    @Override
    public void makeImmutable(String identifier) {
        Preconditions.checkArgument(mutable, "Yet immutable");
        Preconditions.checkArgument(addedColumnsByIdentifier.size() == addedColumns.size(), 
                "Some columns are missing. Found %s but they should be %s", 
                addedColumnsByIdentifier.size(), addedColumns.size());
        this.identifier = identifier;
        this.mutable = false;
        changeConsumer.accept(this);
    }

    @Override
    public String toString() {
        return defautToString();
    }

}
