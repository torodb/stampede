/*
 * ToroDB - ToroDB: Backend common
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.torodb.backend.converters.json;

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonValue;

import com.torodb.backend.converters.ValueConverter;
import com.torodb.backend.converters.array.ArrayConverter;
import com.torodb.backend.converters.array.ValueToArrayConverterProvider;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

/**
 *
 */
public abstract class BaseArrayValueToJsonConverter implements
        ValueConverter<JsonArray, KVArray> {
    private static final long serialVersionUID = 1L;
    
    private final ValueToArrayConverterProvider valueToArrayConverterProvider;
    
    public BaseArrayValueToJsonConverter(ValueToArrayConverterProvider valueToArrayConverterProvider) {
        super();
        this.valueToArrayConverterProvider = valueToArrayConverterProvider;
    }

    @Override
    public Class<? extends JsonArray> getJsonClass() {
        return JsonArray.class;
    }

    @Override
    public Class<? extends KVArray> getValueClass() {
        return KVArray.class;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public KVArray toValue(JsonArray value) {
        List<KVValue<?>> list = new ArrayList<>(value.size());
        for (JsonValue child : value) {
            ArrayConverter converter = valueToArrayConverterProvider.fromJsonValue(child);
            list.add(converter.fromJsonValue(child));
        }
        return new ListKVArray(list);
    }
}
