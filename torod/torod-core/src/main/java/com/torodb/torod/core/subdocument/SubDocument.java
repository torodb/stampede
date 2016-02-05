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
import com.torodb.torod.core.subdocument.values.ScalarValue;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.inject.Provider;

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
    private final Map<String, ScalarValue<?>> values;
    private final SubDocType type;

    private SubDocument(int documentId, int index, SubDocType type, Map<String, ScalarValue<?>> values) {
        this.documentId = documentId;
        this.index = index;
        this.type = type;

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

    @Nonnull
    public ScalarValue<?> getValue(String key) {
        ScalarValue<?> value = values.get(key);
        if (value == null) {
            throw new IllegalArgumentException(key + " is not a key in this subdocument");
        }
        return value;
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
        if (this.type != other.type && (this.type == null || !this.type.equals(other.type))) {
            return false;
        }
        return true;
    }

    public static abstract class Builder {

        int documentId;
        int index;
        private final Map<String, ScalarValue<?>> values;

        private Builder() {
            values = Maps.newHashMap();
        }

        public static Builder withKnownType(SubDocType expectedType) {
            return new WithTypeBuilder(expectedType);
        }

        public static Builder withUnknownType(Provider<SubDocType.Builder> subDocTypeBuilder) {
            return new WithoutTypeBuilder(subDocTypeBuilder.get());
        }
        
        public Builder setDocumentId(int docId) {
            this.documentId = docId;
            return this;
        }
        
        public Builder setIndex(int index) {
            this.index = index;
            return this;
        }

        public Builder add(@Nonnull String key, @Nonnull ScalarValue value) {
            if (values.containsKey(key)) {
                throw new IllegalArgumentException("There is another attribute with " + key);
            }

            values.put(key, value);

            return this;
        }

        public Builder add(@Nonnull SubDocAttribute att, @Nonnull ScalarValue value) {
            if (values.containsKey(att.getKey())) {
                throw new IllegalArgumentException("There is another attribute with " + att.getKey());
            }
            if (!att.getType().equals(value.getType())) {
                throw new IllegalArgumentException("Type of attribute " + att + " is " + att.getType() + " which is different "
                        + "than the type of the given value (value is " + value + " and its type is " + value.getType());
            }
            
            values.put(att.getKey(), value);

            return this;
        }

        abstract SubDocType calculeType();

        public SubDocument build() {
            SubDocument result = new SubDocument(documentId, index, calculeType(), values);
            
            return result;
        }
    }

    private static class WithTypeBuilder extends Builder {
        private final SubDocType expectedType;

        public WithTypeBuilder(SubDocType expectedType) {
            this.expectedType = expectedType;
        }

        @Override
        SubDocType calculeType() {
            return expectedType;
        }

        @Override
        public Builder add(SubDocAttribute att, ScalarValue value) {
            String key = att.getKey();
            Preconditions.checkArgument(att.equals(expectedType.getAttribute(key)));
            return super.add(att, value);
        }

        @Override
        public Builder add(String key, ScalarValue value) {
            Preconditions.checkArgument(expectedType.getAttribute(key) != null);
            return super.add(key, value);
        }
    }

    private static class WithoutTypeBuilder extends Builder {
        private final Map<String, SubDocAttribute> attributes;
        private final SubDocType.Builder subDocTypeBuilder;

        public WithoutTypeBuilder(SubDocType.Builder subDocTypeBuilder) {
            attributes = Maps.newHashMap();
            this.subDocTypeBuilder = subDocTypeBuilder;
        }

        @Override
        SubDocType calculeType() {
            for (SubDocAttribute att : attributes.values()) {
                subDocTypeBuilder.add(att);
            }

            return subDocTypeBuilder.build();
        }

        @Override
        public Builder add(SubDocAttribute att, ScalarValue value) {
            super.add(att, value);
            attributes.put(att.getKey(), att);
            return this;
        }

        @Override
        public Builder add(String key, ScalarValue value) {
            super.add(key, value);
            attributes.put(key, new SubDocAttribute(key, value.getType()));
            return this;
        }

    }

}
