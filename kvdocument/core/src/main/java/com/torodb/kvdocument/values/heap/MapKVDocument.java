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

package com.torodb.kvdocument.values.heap;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KVDocument;
import com.torodb.kvdocument.values.KVValue;
import java.util.LinkedHashMap;


/**
 *
 */
public class MapKVDocument extends KVDocument {

    private static final long serialVersionUID = -5654643148723237245L;

    private final LinkedHashMap<String, KVValue<?>> map;

    public MapKVDocument(@NotMutable LinkedHashMap<String, KVValue<?>> map) {
        this.map = map;
    }

    @Override
    public UnmodifiableIterator<DocEntry<?>> iterator() {
        return Iterators.unmodifiableIterator(
                Iterators.transform(map.entrySet().iterator(), KVDocument.FromEntryMap.INSTANCE)
        );
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public KVValue<?> get(String key) {
        return map.get(key);
    }
}
