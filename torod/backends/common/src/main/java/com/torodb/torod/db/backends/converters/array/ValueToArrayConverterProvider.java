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

package com.torodb.torod.db.backends.converters.array;

import com.torodb.torod.db.backends.converters.json.MongoTimestampValueToJsonConverter;
import com.google.common.collect.Maps;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.*;
import com.torodb.torod.db.backends.converters.ValueConverter;
import com.torodb.torod.db.backends.converters.json.*;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.json.*;

/**
 *
 */
public class ValueToArrayConverterProvider {

    private final Map<ScalarType, ValueConverter> converters;
    private final ValueConverter<JsonArray, ScalarArray> arrayConverter;
    private final ValueConverter<Boolean, ScalarBoolean> booleanConverter;
    private final ValueConverter<String, ScalarDate> dateConverter;
    private final ValueConverter<String, ScalarInstant> dateTimeConverter;
    private final DoubleValueToJsonConverter doubleConverter;
    private final ValueConverter<Number, ScalarInteger> integerConverter;
    private final ValueConverter<Number, ScalarLong> longConverter;
    private final ValueConverter<Void, ScalarNull> nullConverter;
    private final ValueConverter<String, ScalarString> stringConverter;
    private final ValueConverter<String, ScalarTime> timeConverter;
    private final ValueConverter<String, ScalarMongoObjectId> mongoObjectIdConverter;
    private final MongoTimestampValueToJsonConverter mongoTimestampConverter;
    private final ValueConverter<String, ScalarBinary> binaryConverter;

    private ValueToArrayConverterProvider() {
        arrayConverter = new ArrayValueToJsonConverter();
        booleanConverter = new BooleanValueToJsonConverter();
        dateConverter = new DateValueToJsonConverter();
        dateTimeConverter = new InstantValueToJsonConverter();
        doubleConverter = new DoubleValueToJsonConverter();
        integerConverter = new IntegerValueToJsonConverter();
        longConverter = new LongValueToJsonConverter();
        nullConverter = new NullValueToJsonConverter();
        stringConverter = new StringValueToJsonConverter();
        timeConverter = new TimeValueToJsonConverter();
        mongoObjectIdConverter = new MongoObjectIdToArrayConverter();
        mongoTimestampConverter = new MongoTimestampValueToJsonConverter();
        binaryConverter = new BinaryValueToJsonConverter();

        converters = Maps.newEnumMap(ScalarType.class);
        converters.put(ScalarType.ARRAY, arrayConverter);
        converters.put(ScalarType.BOOLEAN, booleanConverter);
        converters.put(ScalarType.DATE, dateConverter);
        converters.put(ScalarType.INSTANT, dateTimeConverter);
        converters.put(ScalarType.DOUBLE, doubleConverter);
        converters.put(ScalarType.INTEGER, integerConverter);
        converters.put(ScalarType.LONG, longConverter);
        converters.put(ScalarType.NULL, nullConverter);
        converters.put(ScalarType.STRING, stringConverter);
        converters.put(ScalarType.TIME, timeConverter);
        converters.put(ScalarType.MONGO_OBJECT_ID, mongoObjectIdConverter);
        converters.put(ScalarType.MONGO_TIMESTAMP, mongoTimestampConverter);
        converters.put(ScalarType.BINARY, binaryConverter);

        
    }

    public static ValueToArrayConverterProvider getInstance() {
        return ToArrayConverterHolder.INSTANCE;
    }

    @Nonnull
    public ValueConverter getConverter(ScalarType valueType) {
        ValueConverter converter = converters.get(valueType);
        if (converter == null) {
            throw new AssertionError("There is no converter that converts "
                    + "elements of type " + valueType);
        }
        return converter;
    }

    @Nonnull
    public ScalarValue<?> convertFromJson(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case ARRAY:
                assert jsonValue instanceof JsonArray;
                return arrayConverter.toValue((JsonArray) jsonValue);
            case TRUE:
                return booleanConverter.toValue(true);
            case FALSE:
                return booleanConverter.toValue(false);
            case NULL:
                return nullConverter.toValue(null);
            case NUMBER:
                assert jsonValue instanceof JsonNumber;
                JsonNumber number = (JsonNumber) jsonValue;
                if (number.isIntegral()) {
                    try {
                        long l = number.longValueExact();
                        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                            return longConverter.toValue(number.longValueExact());
                        }
                        return integerConverter.toValue(number.intValueExact());
                    }
                    catch (ArithmeticException ex) {
                        throw new ToroImplementationException(
                                "Unexpected integral value. " + number + " is "
                                + "bigger than long values"
                        );
                    }
                }
                return doubleConverter.toValue(number.doubleValue());
            case STRING:
                assert jsonValue instanceof JsonString;
                return stringConverter.toValue(((JsonString) jsonValue).getString());
            case OBJECT: {
                JsonObject asObject = ((JsonObject) jsonValue);
                if (mongoTimestampConverter.isValid(asObject)) {
                    return mongoTimestampConverter.toValue(asObject);
                }
                throw new IllegalArgumentException("Te recived JsonObject " + jsonValue
                        + " was not recognized as a valid ScalarValue codification");
            }
            default:
                throw new IllegalArgumentException("Instances of '"
                        + jsonValue.getClass() + "' like '" + jsonValue
                        + "' are not supported");
        }
    }

    public ValueConverter<JsonArray, ScalarArray> getArrayConverter() {
        return arrayConverter;
    }

    public ValueConverter<Boolean, ScalarBoolean> getBooleanConverter() {
        return booleanConverter;
    }

    public ValueConverter<String, ScalarDate> getDateConverter() {
        return dateConverter;
    }

    public ValueConverter<String, ScalarInstant> getInstantConverter() {
        return dateTimeConverter;
    }

    public DoubleValueToJsonConverter getDoubleConverter() {
        return doubleConverter;
    }

    public ValueConverter<Number, ScalarInteger> getIntegerConverter() {
        return integerConverter;
    }

    public ValueConverter<Number, ScalarLong> getLongConverter() {
        return longConverter;
    }

    public ValueConverter<Void, ScalarNull> getNullConverter() {
        return nullConverter;
    }

    public ValueConverter<String, ScalarString> getStringConverter() {
        return stringConverter;
    }

    public ValueConverter<String, ScalarTime> getTimeConverter() {
        return timeConverter;
    }

    public ValueConverter<String, ScalarMongoObjectId> getMongoObjectIdConverter() {
        return mongoObjectIdConverter;
    }
    
    public ValueConverter<JsonObject, ScalarMongoTimestamp> getMongoTimestampConverter() {
        return mongoTimestampConverter;
    }
    
    public ValueConverter<String, ScalarBinary> getBinaryConverter() {
        return binaryConverter;
    }

    private static class ToArrayConverterHolder {

        private static final ValueToArrayConverterProvider INSTANCE
                = new ValueToArrayConverterProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return ValueToArrayConverterProvider.getInstance();
    }
}
