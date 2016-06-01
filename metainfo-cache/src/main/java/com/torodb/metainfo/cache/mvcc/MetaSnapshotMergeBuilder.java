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

package com.torodb.metainfo.cache.mvcc;

import com.google.common.base.Preconditions;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MetaSnapshotMergeBuilder {

    private final ImmutableMetaSnapshot currentSnapshot;
    private final Map<String, ImmutableMetaDatabase> newDatabasesById;
    private boolean built = false;

    public MetaSnapshotMergeBuilder(ImmutableMetaSnapshot currentSnapshot) {
        this.currentSnapshot = currentSnapshot;
        this.newDatabasesById = new HashMap<>();
    }

    public void addModifiedDatabase(MutableMetaDatabase modifiedDatabase) {
        Preconditions.checkState(!built, "This builder has already been built");
        ImmutableMetaDatabase oldDatabase = currentSnapshot.getMetaDatabaseByIdentifier(
                modifiedDatabase.getIdentifier());
        if (oldDatabase == null) {
            assert currentSnapshot.getMetaDatabaseByName(modifiedDatabase.getName()) == null :
                    "Unexpected database with same name but different id";
            newDatabasesById.put(modifiedDatabase.getIdentifier(), modifiedDatabase.immutableCopy());
        } else {
            assert oldDatabase.equals(currentSnapshot.getMetaDatabaseByName(modifiedDatabase.getName())) :
                    "Unexpected database with same id but different name";
            MetaDatabaseMergerBuilder dbMerger = new MetaDatabaseMergerBuilder(oldDatabase);
            for (MutableMetaCollection modifiedCollection : modifiedDatabase.getModifiedCollections()) {
                dbMerger.addModifiedCollection(modifiedCollection);
            }
            newDatabasesById.put(modifiedDatabase.getIdentifier(), dbMerger.build());
        }
    }

    public ImmutableMetaSnapshot build() {
        Preconditions.checkState(!built, "This builder has already been built");
        built = true;

        ImmutableMetaSnapshot.Builder builder = new ImmutableMetaSnapshot.Builder(currentSnapshot);
        for (ImmutableMetaDatabase value : newDatabasesById.values()) {
            builder.add(value);
        }
        return builder.build();
    }



}
