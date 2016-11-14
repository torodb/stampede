/*
 * ToroDB - ToroDB-poc: Backend common
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

import com.torodb.backend.converters.ValueConverter;
import com.torodb.kvdocument.values.KVDouble;

/**
 *
 */
public class DoubleValueToJsonConverter implements
        ValueConverter<Object, KVDouble> {

    private static final long serialVersionUID = 1L;

    @Override
    public Class<? extends Object> getJsonClass() {
        return Double.class;
    }

    @Override
    public Class<? extends KVDouble> getValueClass() {
        return KVDouble.class;
    }

    @Override
    public KVDouble toValue(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            return KVDouble.of(number.doubleValue());
        }
        if (value instanceof String) {
            String string = (String) value;
            if (string.equals("Infinity")) {
                return KVDouble.of(Double.POSITIVE_INFINITY);
            }
            if (string.equals("-Infinity")) {
                return KVDouble.of(Double.NEGATIVE_INFINITY);
            }
            if (string.equals("NaN")) {
                return KVDouble.of(Double.NaN);
            }
        }
        throw new IllegalArgumentException(
                "KVValue "+value+" has not been recognized as double value"
        );
    }
    
}
