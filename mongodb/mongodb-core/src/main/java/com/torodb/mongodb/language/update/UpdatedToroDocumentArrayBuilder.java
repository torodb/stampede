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
 * along with connection. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2016 8Kdata.
 *
 */
package com.torodb.mongodb.language.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import javax.annotation.Nonnegative;

/**
 *
 */
class UpdatedToroDocumentArrayBuilder {

    private boolean built;
    private List<KVValue<?>> values;
    private final Map<Integer, UpdatedToroDocumentArrayBuilder> subArrayBuilders
            = new HashMap<>();
    private final Map<Integer, UpdatedToroDocumentBuilder> subObjectBuilders
            = new HashMap<>();

    private UpdatedToroDocumentArrayBuilder() {
        built = false;
        values = new ArrayList<>();
    }

    public UpdatedToroDocumentArrayBuilder(int expectedSize) {
        built = false;
        values = Lists.newArrayListWithExpectedSize(expectedSize);
    }

    public static UpdatedToroDocumentArrayBuilder from(KVArray original) {
        UpdatedToroDocumentArrayBuilder result = new UpdatedToroDocumentArrayBuilder();
        result.copy(original);

        return result;
    }

    public static UpdatedToroDocumentArrayBuilder create() {
        return new UpdatedToroDocumentArrayBuilder();
    }

    public boolean contains(@Nonnegative int key) {
        return key < values.size() && (
                isValue(key)
                || isArrayBuilder(key)
                || isObjectBuilder(key)
        );
    }

    public boolean isValue(int index) {
        return values.get(index) != null;
    }

    @Nonnull
    public KVValue<?> getValue(int index) {
        KVValue<?> result = values.get(index);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no value associated to '" + index + "' key");
        }
        return result;
    }

    public boolean isArrayBuilder(int index) {
        return subArrayBuilders.containsKey(index);
    }

    @Nonnull
    public UpdatedToroDocumentArrayBuilder getArrayBuilder(int index) {
        UpdatedToroDocumentArrayBuilder result = subArrayBuilders.get(index);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no array builder associated to '" + index + "' key");
        }
        return result;
    }

    public boolean isObjectBuilder(int index) {
        return subObjectBuilders.containsKey(index);
    }

    @Nonnull
    public UpdatedToroDocumentBuilder getObjectBuilder(int index) {
        UpdatedToroDocumentBuilder result = subObjectBuilders.get(index);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no object builder associated to '" + index + "' key");
        }
        return result;
    }

    private void setElement(int index, KVValue<?> element) {
        prepareSize(index);
        values.set(index, element);
        subObjectBuilders.remove(index);
        subArrayBuilders.remove(index);
    }

    private void setArrayBuilder(int index, UpdatedToroDocumentArrayBuilder builder) {
        prepareSize(index);
        values.set(index, null);
        subObjectBuilders.remove(index);
        subArrayBuilders.put(index, builder);
    }

    private void setObjectBuilder(int index, UpdatedToroDocumentBuilder builder) {
        prepareSize(index);
        values.set(index, null);
        subObjectBuilders.put(index, builder);
        subArrayBuilders.remove(index);
    }

    public UpdatedToroDocumentArrayBuilder newArray(int index) {
        checkNewBuild();

        UpdatedToroDocumentArrayBuilder result = new UpdatedToroDocumentArrayBuilder();
        setArrayBuilder(index, result);

        return result;
    }

    public UpdatedToroDocumentBuilder newObject(int index) {
        checkNewBuild();

        UpdatedToroDocumentBuilder result = UpdatedToroDocumentBuilder.create();
        setObjectBuilder(index, result);

        return result;
    }

    public boolean unset(int index) {
        if (values.size() >= index) {
            return false;
        }
        setElement(index, KVNull.getInstance());
        return true;
    }

    public UpdatedToroDocumentArrayBuilder add(KVValue<?> newVal) {
        checkNewBuild();

        values.add(newVal);

        return this;
    }

    public UpdatedToroDocumentArrayBuilder setValue(int index, KVValue<?> newValue) {
        checkNewBuild();

        if (newValue instanceof KVDocument) {
            newObject(index).copy((KVDocument) newValue);
        } else if (newValue instanceof KVArray) {
            newArray(index).copy((KVArray) newValue);
        } else {
            setElement(index, newValue);
        }
        return this;
    }

    public KVArray build() {
        built = true;

        for (Map.Entry<Integer, UpdatedToroDocumentBuilder> objectBuilder
                : subObjectBuilders.entrySet()) {

            KVValue<?> oldValue
                    = values.set(
                            objectBuilder.getKey(),
                            objectBuilder.getValue().buildRoot()
                    );

            assert oldValue == null;
        }
        for (Map.Entry<Integer, UpdatedToroDocumentArrayBuilder> arrayBuilder
                : subArrayBuilders.entrySet()) {

            KVValue<?> oldValue
                    = values.set(
                            arrayBuilder.getKey(),
                            arrayBuilder.getValue().build()
                    );

            assert oldValue == null;
        }
        subObjectBuilders.clear();
        subArrayBuilders.clear();

        return new ListKVArray(values);
    }

    private KVType getType(int index) {
        KVValue<?> value = values.get(index);
        if (value != null) {
            return value.getType();
        }
        if (isObjectBuilder(index)) {
            return DocumentType.INSTANCE;
        } else if (isArrayBuilder(index)) {
            return getArrayBuilder(index).calculateElementType();
        } else {
            throw new IllegalStateException();
        }
    }

    private KVType calculateElementType() {
        KVType result = null;
        for (int i = 0; i < values.size(); i++) {
            KVType iestType = getType(i);
            if (result == null) {
                result = iestType;
            } else if (!result.equals(iestType)) {
                result = GenericType.INSTANCE;
                break;
            }
        }

        return result;
    }

    void copy(KVArray original) {
        values.clear();
        subArrayBuilders.clear();
        subObjectBuilders.clear();

        for (int i = 0; i < original.size(); i++) {
            KVValue<?> value = original.get(i);

            if (value instanceof KVArray) {
                UpdatedToroDocumentArrayBuilder childBuilder = newArray(i);
                childBuilder.copy((KVArray) value);
            } else if (value instanceof KVDocument) {
                UpdatedToroDocumentBuilder childBuilder = newObject(i);
                childBuilder.copy((KVDocument) value);
            } else {
                setValue(i, value);
            }
        }
    }

    private void checkNewBuild() {
        if (built) {
            values = new ArrayList<>();
            built = false;
        }
    }

    private void prepareSize(int minSize) {
        for (int i = values.size(); i <= minSize; i++) {
            values.add(KVNull.getInstance());
        }
    }
}
