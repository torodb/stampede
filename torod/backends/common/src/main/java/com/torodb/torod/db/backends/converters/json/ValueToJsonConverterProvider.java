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

import com.google.common.collect.Maps;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.db.backends.converters.ValueConverter;

import javax.annotation.Nonnull;
import javax.json.JsonArray;
import java.util.Map;

/**
 *
 */
public class ValueToJsonConverterProvider {

    private final Map<BasicType, ValueConverter> converters;
    private final ValueConverter<JsonArray, ArrayValue> arrayConverter;
    private final ValueConverter<Boolean, BooleanValue> booleanConverter;
    private final ValueConverter<String, DateValue> dateConverter;
    private final ValueConverter<String, DateTimeValue> dateTimeConverter;
    private final ValueConverter<Object, DoubleValue> doubleConverter;
    private final ValueConverter<Number, IntegerValue> integerConverter;
    private final ValueConverter<Number, LongValue> longConverter;
    private final ValueConverter<Void, NullValue> nullConverter;
    private final ValueConverter<String, StringValue> stringConverter;
    private final ValueConverter<String, TimeValue> timeConverter;
    private final ValueConverter<String, TwelveBytesValue> twelveBytesConverter;
    private final ValueConverter<String, PatternValue> posixPatternConverter;

    private ValueToJsonConverterProvider() {
        arrayConverter = new ArrayValueToJsonConverter();
        booleanConverter = new BooleanValueToJsonConverter();
        dateConverter = new DateValueToJsonConverter();
        dateTimeConverter = new DateTimeValueToJsonConverter();
        doubleConverter = new DoubleValueToJsonConverter();
        integerConverter = new IntegerValueToJsonConverter();
        longConverter = new LongValueToJsonConverter();
        nullConverter = new NullValueToJsonConverter();
        stringConverter = new StringValueToJsonConverter();
        timeConverter = new TimeValueToJsonConverter();
        twelveBytesConverter = new TwelveBytesValueToJsonConverter();
        posixPatternConverter = new PosixPatternValueToJsonConverter();

        converters = Maps.newEnumMap(BasicType.class);
        converters.put(BasicType.ARRAY, arrayConverter);
        converters.put(BasicType.BOOLEAN, booleanConverter);
        converters.put(BasicType.DATE, dateConverter);
        converters.put(BasicType.DATETIME, dateTimeConverter);
        converters.put(BasicType.DOUBLE, doubleConverter);
        converters.put(BasicType.INTEGER, integerConverter);
        converters.put(BasicType.LONG, longConverter);
        converters.put(BasicType.NULL, nullConverter);
        converters.put(BasicType.STRING, stringConverter);
        converters.put(BasicType.TIME, timeConverter);
        converters.put(BasicType.TWELVE_BYTES, twelveBytesConverter);
        converters.put(BasicType.PATTERN, posixPatternConverter);

    }

    public static ValueToJsonConverterProvider getInstance() {
        return ValueToJsonConverterProviderHolder.INSTANCE;
    }

    @Nonnull
    public ValueConverter getConverter(BasicType valueType) {
        ValueConverter converter = converters.get(valueType);
        if (converter == null) {
            throw new AssertionError("There is no converter that converts "
                    + "elements of type " + valueType);
        }
        return converter;
    }

    private static class ValueToJsonConverterProviderHolder {

        private static final ValueToJsonConverterProvider INSTANCE
                = new ValueToJsonConverterProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return ValueToJsonConverterProvider.getInstance();
    }
}
