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

import com.google.common.collect.Maps;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVDocument.DocEntry;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.MapKVDocument;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 *
 */
class KVDocumentBuilder {

    private LinkedHashMap<String, KVValue<?>> values = Maps.newLinkedHashMap();
    private final Map<String, KVArrayBuilder> subArrayBuilders = Maps.newHashMap();
    private final Map<String, KVDocumentBuilder> subObjectBuilders = Maps.newHashMap();
    private boolean built = false;

    private KVDocumentBuilder() {
    }

    public void clear() {
        if (built) {
            built = false;
            values = Maps.newLinkedHashMap();
        } else {
            values.clear();
        }
        subArrayBuilders.clear();
        subObjectBuilders.clear();
    }

    public static KVDocumentBuilder create() {
        return new KVDocumentBuilder();
    }

    public static KVDocumentBuilder from(KVDocument original) {
        KVDocumentBuilder result = KVDocumentBuilder.create();
        result.copy(original);

        return result;
    }

    public boolean contains(String key) {
        return isValue(key)
                || isArrayBuilder(key)
                || isObjectBuilder(key);
    }

    public boolean isValue(String key) {
        return values.containsKey(key);
    }

    @Nonnull
    public KVValue<?> getValue(String key) {
        KVValue<?> result = values.get(key);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no value associated to '" + key + "' key");
        }
        return result;
    }

    public boolean isArrayBuilder(String key) {
        return subArrayBuilders.containsKey(key);
    }

    @Nonnull
    public KVArrayBuilder getArrayBuilder(String key) {
        KVArrayBuilder result = subArrayBuilders.get(key);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no array builder associated to '" + key + "' key");
        }
        return result;
    }

    public boolean isObjectBuilder(String key) {
        return subObjectBuilders.containsKey(key);
    }

    @Nonnull
    public KVDocumentBuilder getObjectBuilder(String key) {
        KVDocumentBuilder result = subObjectBuilders.get(key);
        if (result == null) {
            throw new IllegalArgumentException(
                    "There is no object builder associated to '" + key + "' key");
        }
        return result;
    }

    public KVDocumentBuilder putValue(String key, KVValue<?> value) {
        checkNewBuild();

        if (value instanceof KVDocument) {
            newObject(key).copy((KVDocument) value);
        } else if (value instanceof KVArray) {
            newArray(key).copy((KVArray) value);
        } else {
            values.put(key, value);
            subArrayBuilders.remove(key);
            subObjectBuilders.remove(key);
        }

        return this;
    }
    
    public KVArrayBuilder newArray(String key) {
        checkNewBuild();

        KVArrayBuilder result = KVArrayBuilder.create();

        values.remove(key);
        subArrayBuilders.put(key, result);
        subObjectBuilders.remove(key);

        return result;
    }

    public KVDocumentBuilder newObject(String key) {
        checkNewBuild();

        KVDocumentBuilder result = KVDocumentBuilder.create();

        values.remove(key);
        subArrayBuilders.remove(key);
        subObjectBuilders.put(key, result);

        return result;
    }

    public boolean unset(String key) {
        boolean result = false;
        result |= values.remove(key) != null;
        result |= subArrayBuilders.remove(key) != null;
        result |= subObjectBuilders.remove(key) != null;

        return result;
    }

    public KVDocument build() {
        built = true;

        for (Map.Entry<String, KVDocumentBuilder> objectBuilder
                : subObjectBuilders.entrySet()) {

            KVValue<?> oldValue
                    = values.put(
                            objectBuilder.getKey(),
                            objectBuilder.getValue().build()
                    );

            assert oldValue == null;
        }
        for (Map.Entry<String, KVArrayBuilder> arrayBuilder
                : subArrayBuilders.entrySet()) {

            KVValue<?> oldValue
                    = values.put(
                            arrayBuilder.getKey(),
                            arrayBuilder.getValue().build()
                    );

            assert oldValue == null;
        }
        subObjectBuilders.clear();
        subArrayBuilders.clear();

        return new MapKVDocument(values);
    }

    void copy(KVDocument original) {
        values.clear();
        subArrayBuilders.clear();
        subObjectBuilders.clear();

        for (DocEntry<?> entry : original) {
            KVValue<?> value = entry.getValue();
            if (value instanceof KVArray) {
                KVArrayBuilder childBuilder = newArray(entry.getKey());
                childBuilder.copy((KVArray) value);
            } else if (value instanceof KVDocument) {
                KVDocumentBuilder childBuilder = newObject(entry.getKey());
                childBuilder.copy((KVDocument) value);
            } else {
                putValue(entry.getKey(), value);
            }
        }
    }

    private void checkNewBuild() {
        if (built) {
            values = Maps.newLinkedHashMap();
            built = false;
        }
    }
}
