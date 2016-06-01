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
import com.torodb.core.transaction.metainf.ImmutableMetaCollection;
import com.torodb.core.transaction.metainf.ImmutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MetaDatabaseMergerBuilder {
    private final ImmutableMetaDatabase currentDatabase;
    private final Map<String, ImmutableMetaCollection> newCollectionsById = new HashMap<>();
    private boolean built = false;

    public MetaDatabaseMergerBuilder(ImmutableMetaDatabase currentDatabase) {
        this.currentDatabase = currentDatabase;
    }

    public void addModifiedCollection(MutableMetaCollection modifiedCollection) {
        Preconditions.checkState(!built, "This builder has already been built");
        ImmutableMetaCollection oldCol = currentDatabase.getMetaCollectionByIdentifier(
                modifiedCollection.getIdentifier());

        if (oldCol == null) {
            assert currentDatabase.getMetaCollectionByName(modifiedCollection.getName()) == null
                    : "Unexpected collection with same name but different id";
            newCollectionsById.put(modifiedCollection.getIdentifier(), modifiedCollection.immutableCopy());
        } else {
            assert oldCol.equals(currentDatabase.getMetaCollectionByName(modifiedCollection.getName()))
                    : "Unexpected collection with same id but different name";
            MetaCollectionMergeBuilder colMerger = new MetaCollectionMergeBuilder(oldCol);
            for (MutableMetaDocPart modifiedDocPart : modifiedCollection.getModifiedMetaDocParts()) {
                colMerger.addDocPart(modifiedDocPart);
            }
            newCollectionsById.put(modifiedCollection.getIdentifier(), colMerger.build());
        }
    }

    public ImmutableMetaDatabase build() {
        Preconditions.checkState(!built, "This builder has already been built");
        built = true;

        ImmutableMetaDatabase.Builder builder = new ImmutableMetaDatabase.Builder(currentDatabase);
        for (ImmutableMetaCollection value : newCollectionsById.values()) {
            builder.add(value);
        }
        return builder.build();
    }
}
