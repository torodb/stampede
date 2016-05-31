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
import com.torodb.core.transaction.metainf.ImmutableMetaDocPart;
import com.torodb.core.transaction.metainf.ImmutableMetaField;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MetaDocPartMergerBuilder {
    private final ImmutableMetaDocPart currentDocPart;
    private final Map<String, ImmutableMetaField> newFieldsById = new HashMap<>();
    private boolean built = false;

    public MetaDocPartMergerBuilder(ImmutableMetaDocPart currentDatabase) {
        this.currentDocPart = currentDatabase;
    }

    public void addField(ImmutableMetaField newField) {
        Preconditions.checkState(!built, "This builder has already been built");
        ImmutableMetaField oldField = currentDocPart.getMetaFieldByIdentifier(newField.getIdentifier());

        if (oldField == null) {
            assert currentDocPart.getMetaFieldByNameAndType(newField.getName(), newField.getType()) == null
                    : "Unexpected field with same name and type but different id";
            newFieldsById.put(newField.getIdentifier(), newField);
        } else {
            assert oldField.equals(currentDocPart.getMetaFieldByNameAndType(newField.getName(), newField.getType()))
                    : "Unexpected field with same id but different name";
        }
    }

    public ImmutableMetaDocPart build() {
        Preconditions.checkState(!built, "This builder has already been built");
        built = true;

        ImmutableMetaDocPart.Builder builder = new ImmutableMetaDocPart.Builder(currentDocPart);
        for (ImmutableMetaField value : newFieldsById.values()) {
            builder.add(value);
        }
        return builder.build();
    }
}
