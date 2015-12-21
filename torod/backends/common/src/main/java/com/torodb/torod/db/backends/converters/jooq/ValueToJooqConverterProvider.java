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

package com.torodb.torod.db.backends.converters.jooq;

import com.torodb.torod.core.subdocument.values.Value;
import com.torodb.torod.core.subdocument.BasicType;

import java.io.Serializable;
import java.util.EnumMap;

/**
 *
 */
public class ValueToJooqConverterProvider {

    private static final EnumMap<BasicType, SubdocValueConverter> converters;

    static {
        converters = new EnumMap<BasicType, SubdocValueConverter>(BasicType.class);

        converters.put(BasicType.ARRAY, new ArrayValueConverter());
        converters.put(BasicType.BOOLEAN, new BooleanValueConverter());
        converters.put(BasicType.DOUBLE, new DoubleValueConverter());
        converters.put(BasicType.INTEGER, new IntegerValueConverter());
        converters.put(BasicType.LONG, new LongValueConverter());
        converters.put(BasicType.NULL, new NullValueConverter());
        converters.put(BasicType.STRING, new StringValueConverter());
        converters.put(BasicType.DATE, new DateValueConverter());
        converters.put(BasicType.DATETIME, new DateTimeValueConverter());
        converters.put(BasicType.TIME, new TimeValueConverter());
        converters.put(BasicType.TWELVE_BYTES, new TwelveBytesValueConverter());
        converters.put(BasicType.PATTERN, new PatternValueConverter());
    }

    public static SubdocValueConverter<?, ? extends Value<? extends Serializable>> getConverter(BasicType type) {
        SubdocValueConverter converter = converters.get(type);
        if (converter == null) {
            throw new IllegalArgumentException("It is not defined how to map elements of type " + type + " to SQL");
        }
        return converter;
    }
}
