/*
 *     This file is part of ToroDB.
 *
 *     ToroDB is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     ToroDB is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with ToroDB. If not, see <http://www.gnu.org/licenses/>.
 *
 *     Copyright (c) 2014, 8Kdata Technology
 *     
 */

package com.torodb.torod.core.subdocument;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.torodb.torod.core.subdocument.values.Value;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class SubDocument {

    /**
     * The id of the document that contains this document.
     */
    private final int documentId;
    /**
     * The index of this subdocument related to the other subDocuments with the same subDocument type.
     */
    private final int index;
    private final Map<String, Value<?>> values;
    private final Map<String, SubDocAttribute> attributes;
    private final SubDocType type;

    private SubDocument(int documentId, int index, SubDocType type, Map<String, SubDocAttribute> attributes, Map<String, Value<?>> values) {
        this.documentId = documentId;
        this.index = index;
        this.type = type;

        this.attributes = Collections.unmodifiableMap(attributes);
        this.values = Collections.unmodifiableMap(values);
    }

    public int getDocumentId() {
        return documentId;
    }

    public int getIndex() {
        return index;
    }

    public SubDocType getType() {
        return type;
    }

    public Map<String, SubDocAttribute> getAttributes() {
        return attributes;
    }

    @Nonnull
    public Value<?> getValue(String key) {
        if (!values.containsKey(key)) {
            throw new IllegalArgumentException(key + " is not a key in this subdocument");
        }
        return values.get(key);
    }

    public int size() {
        return values.size();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + this.documentId;
        hash = 79 * hash + this.index;
        hash = 79 * hash + (this.type != null ? this.type.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SubDocument other = (SubDocument) obj;
        if (this.documentId != other.documentId) {
            return false;
        }
        if (this.index != other.index) {
            return false;
        }
        if (this.values != other.values && (this.values == null || !this.values.equals(other.values))) {
            return false;
        }
        if (this.attributes != other.attributes && (this.attributes == null || !this.attributes.equals(other.attributes))) {
            return false;
        }
        if (this.type != other.type && (this.type == null || !this.type.equals(other.type))) {
            return false;
        }
        return true;
    }

    public static class Builder {

        int documentId;
        int index;
        private Map<String, Value<?>> values;
        private Map<String, SubDocAttribute> attributes;

        public Builder() {
            attributes = Maps.newHashMap();
            values = Maps.newHashMap();
        }
        
        public Builder setDocumentId(int docId) {
            this.documentId = docId;
            return this;
        }
        
        public Builder setIndex(int index) {
            this.index = index;
            return this;
        }

        public Builder add(@Nonnull String key, @Nonnull Value value) {
            Preconditions.checkArgument(!attributes.containsKey(key), "There is another attribute with " + key);

            attributes.put(key, new SubDocAttribute(key, value.getType()));
            values.put(key, value);
            return this;
        }

        public Builder add(@Nonnull SubDocAttribute att, @Nonnull Value value) {
            if (!att.getType().equals(value.getType())) {
                throw new IllegalArgumentException("Type of attribute " + att + " is " + att.getType() + " which is different "
                        + "than the type of the given value (value is " + value + " and its type is " + value.getType());
            }
            
            attributes.put(att.getKey(), att);
            values.put(att.getKey(), value);
            return this;
        }

        public SubDocType calculeType() {
            SubDocType.Builder subDocTypeBuilder = new SubDocType.Builder();

            for (SubDocAttribute att : attributes.values()) {
                subDocTypeBuilder.add(att);
            }

            return subDocTypeBuilder.build();
        }

        public SubDocument build() {
            SubDocument result = new SubDocument(documentId, index, calculeType(), attributes, values);
            
            values = null;
            attributes = null;
            
            return result;
        }
    }

}
