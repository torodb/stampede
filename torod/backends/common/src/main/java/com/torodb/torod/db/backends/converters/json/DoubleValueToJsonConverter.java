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

package com.torodb.torod.db.backends.converters.json;

import com.torodb.torod.db.backends.converters.ValueConverter;
import com.torodb.torod.core.subdocument.values.DoubleValue;

/**
 *
 */
public class DoubleValueToJsonConverter implements
        ValueConverter<Object, DoubleValue> {

    @Override
    public Class<? extends Object> getJsonClass() {
        return Double.class;
    }

    @Override
    public Class<? extends DoubleValue> getValueClass() {
        return DoubleValue.class;
    }

    @Override
    public Number toJson(DoubleValue value) {
        return value.getValue();
    }

    @Override
    public DoubleValue toValue(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            return new DoubleValue(number.doubleValue());
        }
        if (value instanceof String) {
            String string = (String) value;
            if (string.equals("Infinity")) {
                return new DoubleValue(Double.POSITIVE_INFINITY);
            }
            if (string.equals("-Infinity")) {
                return new DoubleValue(Double.NEGATIVE_INFINITY);
            }
            if (string.equals("NaN")) {
                return new DoubleValue(Double.NaN);
            }
        }
        throw new IllegalArgumentException(
                "Value "+value+" has not been recognized as double value"
        );
    }
    
}
