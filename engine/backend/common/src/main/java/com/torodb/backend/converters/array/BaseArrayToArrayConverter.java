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
package com.torodb.backend.converters.array;

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonValue;

import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVValue;
import com.torodb.kvdocument.values.heap.ListKVArray;

/**
 *
 */
public abstract class BaseArrayToArrayConverter implements ArrayConverter<JsonArray, KVArray> {
    private static final long serialVersionUID = 1L;
    
    private final ValueToArrayConverterProvider valueToArrayConverterProvider;

    public BaseArrayToArrayConverter(ValueToArrayConverterProvider valueToArrayConverterProvider) {
        super();
        this.valueToArrayConverterProvider = valueToArrayConverterProvider;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public String toJsonLiteral(KVArray value) {
        StringBuilder sb = new StringBuilder(value.size() * 20);
        sb.append("[");
        for (KVValue<?> child : value) {
            sb.append(((ArrayConverter) valueToArrayConverterProvider.getConverter(child.getType())).toJsonLiteral(child));
            sb.append(",");
        }
        if (!value.isEmpty()) {
            sb.delete(sb.length()-1, sb.length());
        }
        sb.append("]");
        return sb.toString();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public KVArray fromJsonValue(JsonArray value) {
        List<KVValue<?>> list = new ArrayList<>(value.size());
        for (JsonValue child : value) {
            ArrayConverter converter = valueToArrayConverterProvider.fromJsonValue(child);
            list.add(converter.fromJsonValue(child));
        }
        return new ListKVArray(list);
    }
}
