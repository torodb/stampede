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

import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class WrapperMutableMetaSnapshot implements MutableMetaSnapshot<MutableMetaDatabase>{

    private final Map<String, MutableMetaDatabase> newDatabases;

    public WrapperMutableMetaSnapshot(MetaSnapshot<?> originalSnapshot) {
        this.newDatabases = new HashMap<>();

        originalSnapshot.streamDatabases().forEach((db) -> {
                @SuppressWarnings("unchecked")
                MutableMetaDatabase mutable = new WrapperMutableMetaDatabase(db);
                newDatabases.put(db.getName(), mutable);
        });
    }

    @Override
    public MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws
            IllegalArgumentException {
        if (getMetaDatabaseByName(dbName) != null) {
            throw new IllegalArgumentException("There is another database whose name is " + dbName);
        }

        assert getMetaDatabaseByIdentifier(dbId) == null : "There is another database whose id is " + dbId;

        MutableMetaDatabase result = new WrapperMutableMetaDatabase(
                new ImmutableMetaDatabase(dbName, dbId, Collections.emptyList())
        );

        newDatabases.put(dbName, result);

        return result;
    }

    @Override
    public Stream<MutableMetaDatabase> streamDatabases() {
        return newDatabases.values().stream();
    }

    @Override
    public MutableMetaDatabase getMetaDatabaseByName(String dbName) {
        return newDatabases.get(dbName);
    }

    @Override
    public MutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier) {
        return newDatabases.values().stream()
                .filter((db) -> db.getIdentifier().equals(dbIdentifier))
                .findAny()
                .orElse(null);
    }


}
