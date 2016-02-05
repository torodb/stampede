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

import com.torodb.torod.core.subdocument.values.ScalarDouble;
import com.torodb.torod.db.backends.converters.ValueConverter;

/**
 *
 */
public class DoubleValueToJsonConverter implements
        ValueConverter<Object, ScalarDouble> {

    @Override
    public Class<? extends Object> getJsonClass() {
        return Double.class;
    }

    @Override
    public Class<? extends ScalarDouble> getValueClass() {
        return ScalarDouble.class;
    }

    @Override
    public Number toJson(ScalarDouble value) {
        return value.getValue();
    }

    @Override
    public ScalarDouble toValue(Object value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            return ScalarDouble.of(number.doubleValue());
        }
        if (value instanceof String) {
            String string = (String) value;
            if (string.equals("Infinity")) {
                return ScalarDouble.of(Double.POSITIVE_INFINITY);
            }
            if (string.equals("-Infinity")) {
                return ScalarDouble.of(Double.NEGATIVE_INFINITY);
            }
            if (string.equals("NaN")) {
                return ScalarDouble.of(Double.NaN);
            }
        }
        throw new IllegalArgumentException(
                "ScalarValue "+value+" has not been recognized as double value"
        );
    }
    
}
