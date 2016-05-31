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
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import com.torodb.core.transaction.metainf.MutableMetaDocPart;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MetaCollectionMergeBuilder {
    private final ImmutableMetaCollection currentCollection;
    private final Map<String, ImmutableMetaDocPart> newDocPartsById = new HashMap<>();
    private boolean built = false;

    public MetaCollectionMergeBuilder(ImmutableMetaCollection currentCollection) {
        this.currentCollection = currentCollection;
    }

    public void addDocPart(MutableMetaDocPart modifiedDocPart) {
        Preconditions.checkState(!built, "This builder has already been built");
        ImmutableMetaDocPart oldDocPart = currentCollection.getMetaDocPartByIdentifier(modifiedDocPart.getIdentifier());

        if (oldDocPart == null) {
            assert currentCollection.getMetaDocPartByTableRef(modifiedDocPart.getTableRef()) == null
                    : "Unexpected doc part with same ref but different id";
            newDocPartsById.put(modifiedDocPart.getIdentifier(), modifiedDocPart.immutableCopy());
        } else {
            assert oldDocPart.equals(currentCollection.getMetaDocPartByTableRef(modifiedDocPart.getTableRef()))
                    : "Unexpected doc part with same ref but different id";
            MetaDocPartMergerBuilder docPartMerger = new MetaDocPartMergerBuilder(oldDocPart);
            for (ImmutableMetaField addedField : modifiedDocPart.getAddedMetaFields()) {
                docPartMerger.addField(addedField);
            }
            newDocPartsById.put(modifiedDocPart.getIdentifier(), docPartMerger.build());
        }
    }

    public ImmutableMetaCollection build() {
        Preconditions.checkState(!built, "This builder has already been built");
        built = true;

        ImmutableMetaCollection.Builder builder = new ImmutableMetaCollection.Builder(currentCollection);
        for (ImmutableMetaDocPart value : newDocPartsById.values()) {
            builder.add(value);
        }
        return builder.build();
    }

}
