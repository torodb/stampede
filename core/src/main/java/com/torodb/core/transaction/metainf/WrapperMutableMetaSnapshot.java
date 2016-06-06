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
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaSnapshot implements MutableMetaSnapshot {

    private final ImmutableMetaSnapshot wrapped;
    private final Map<String, WrapperMutableMetaDatabase> newDatabases;
    private final Set<WrapperMutableMetaDatabase> changedDatabases;

    public WrapperMutableMetaSnapshot(ImmutableMetaSnapshot wrapped) {
        this.wrapped = wrapped;
        this.newDatabases = new HashMap<>();

        changedDatabases = new HashSet<>();

        wrapped.streamMetaDatabases().forEach((db) -> {
                @SuppressWarnings("unchecked")
                WrapperMutableMetaDatabase mutable = createMetaDatabase(db, false);
                newDatabases.put(db.getName(), mutable);
        });
    }

    protected WrapperMutableMetaDatabase createMetaDatabase(ImmutableMetaDatabase immutable, boolean isNew) {
        return new WrapperMutableMetaDatabase(immutable, this::onMetaDatabaseChange, isNew);
    }

    @Override
    public MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws
            IllegalArgumentException {
        if (getMetaDatabaseByName(dbName) != null) {
            throw new IllegalArgumentException("There is another database whose name is " + dbName);
        }

        assert getMetaDatabaseByIdentifier(dbId) == null : "There is another database whose id is " + dbId;

        WrapperMutableMetaDatabase result = createMetaDatabase(
                new ImmutableMetaDatabase(dbName, dbId, Collections.emptyList()), 
                true
        );

        newDatabases.put(dbName, result);
        onMetaDatabaseChange(result);

        return result;
    }

    @DoNotChange
    @Override
    public Iterable<? extends WrapperMutableMetaDatabase> getModifiedDatabases() {
        return changedDatabases;
    }

    @Override
    public ImmutableMetaSnapshot immutableCopy() {
        if (changedDatabases.isEmpty()) {
            return wrapped;
        } else {
            ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(wrapped);
            for (MutableMetaDatabase changedDatabase : changedDatabases) {
                builder.add(changedDatabase.immutableCopy());
            }
            return builder.build();
        }
    }

    @Override
    public Stream<? extends WrapperMutableMetaDatabase> streamMetaDatabases() {
        return newDatabases.values().stream();
    }

    @Override
    public WrapperMutableMetaDatabase getMetaDatabaseByName(String dbName) {
        return newDatabases.get(dbName);
    }

    @Override
    public WrapperMutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier) {
        return newDatabases.values().stream()
                .filter((db) -> db.getIdentifier().equals(dbIdentifier))
                .findAny()
                .orElse(null);
    }

    private void onMetaDatabaseChange(WrapperMutableMetaDatabase changed) {
        changedDatabases.add(changed);
    }

}
