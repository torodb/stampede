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
package com.toro.torod.connection.update;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.torodb.kvdocument.types.DocumentType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 *
 */
class KVArrayBuilder {

    private boolean built;
    private List<KVValue<?>> values;
    private final Map<Integer, KVArrayBuilder> subArrayBuilders
            = Maps.newHashMap();
    private final Map<Integer, KVDocumentBuilder> subObjectBuilders
            = Maps.newHashMap();

    private KVArrayBuilder() {
        built = false;
        values = Lists.newArrayList();
    }

    public KVArrayBuilder(int expectedSize) {
        built = false;
        values = Lists.newArrayListWithExpectedSize(expectedSize);
    }

    public static KVArrayBuilder from(KVArray original) {
        KVArrayBuilder result = new KVArrayBuilder();
        result.copy(original);

        return result;
    }

    public static KVArrayBuilder create() {
        return new KVArrayBuilder();
    }

    public boolean contains(int key) {
        return isValue(key)
                || isArrayBuilder(key)
                || isObjectBuilder(key);
    }

    public boolean isValue(int index) {
        return values.get(index) != null;
    }

    @Nonnull
    public KVValue getValue(int index) {
        KVValue result = values.get(index);
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
    public KVArrayBuilder getArrayBuilder(int index) {
        KVArrayBuilder result = subArrayBuilders.get(index);
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
    public KVDocumentBuilder getObjectBuilder(int index) {
        KVDocumentBuilder result = subObjectBuilders.get(index);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no object builder associated to '" + index + "' key");
        }
        return result;
    }

    private void setElement(int index, KVValue element) {
        prepareSize(index);
        values.set(index, element);
        subObjectBuilders.remove(index);
        subArrayBuilders.remove(index);
    }

    private void setArrayBuilder(int index, KVArrayBuilder builder) {
        prepareSize(index);
        values.set(index, null);
        subObjectBuilders.remove(index);
        subArrayBuilders.put(index, builder);
    }

    private void setObjectBuilder(int index, KVDocumentBuilder builder) {
        prepareSize(index);
        values.set(index, null);
        subObjectBuilders.put(index, builder);
        subArrayBuilders.remove(index);
    }

    public KVArrayBuilder newArray(int index) {
        checkNewBuild();

        KVArrayBuilder result = new KVArrayBuilder();
        setArrayBuilder(index, result);

        return result;
    }

    public KVDocumentBuilder newObject(int index) {
        checkNewBuild();

        KVDocumentBuilder result = KVDocumentBuilder.create();
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

    public KVArrayBuilder add(KVValue newVal) {
        checkNewBuild();

        values.add(newVal);

        return this;
    }

    public KVArrayBuilder setValue(int index, KVValue newValue) {
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

        for (Map.Entry<Integer, KVDocumentBuilder> objectBuilder
                : subObjectBuilders.entrySet()) {

            KVValue oldValue
                    = values.set(
                            objectBuilder.getKey(),
                            objectBuilder.getValue().build()
                    );

            assert oldValue == null;
        }
        for (Map.Entry<Integer, KVArrayBuilder> arrayBuilder
                : subArrayBuilders.entrySet()) {

            KVValue oldValue
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
        KVValue value = values.get(index);
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
            KVValue value = original.get(i);

            if (value instanceof KVArray) {
                KVArrayBuilder childBuilder = newArray(i);
                childBuilder.copy((KVArray) value);
            } else if (value instanceof KVDocument) {
                KVDocumentBuilder childBuilder = newObject(i);
                childBuilder.copy((KVDocument) value);
            } else {
                setValue(i, value);
            }
        }
    }

    private void checkNewBuild() {
        if (built) {
            values = Lists.newArrayList();
            built = false;
        }
    }

    private void prepareSize(int minSize) {
        for (int i = values.size(); i <= minSize; i++) {
            values.add(KVNull.getInstance());
        }
    }
}
