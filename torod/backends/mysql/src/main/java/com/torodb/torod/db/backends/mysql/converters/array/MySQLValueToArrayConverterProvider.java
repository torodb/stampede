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

package com.torodb.torod.db.backends.mysql.converters.array;

import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import com.google.common.collect.Maps;
import com.torodb.torod.core.exceptions.ToroImplementationException;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.ScalarBoolean;
import com.torodb.torod.core.subdocument.values.ScalarDate;
import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.subdocument.values.ScalarLong;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarString;
import com.torodb.torod.core.subdocument.values.ScalarTime;
import com.torodb.torod.db.backends.converters.array.ArrayConverter;
import com.torodb.torod.db.backends.converters.array.BooleanToArrayConverter;
import com.torodb.torod.db.backends.converters.array.DateToArrayConverter;
import com.torodb.torod.db.backends.converters.array.DoubleToArrayConverter;
import com.torodb.torod.db.backends.converters.array.IntegerToArrayConverter;
import com.torodb.torod.db.backends.converters.array.LongToArrayConverter;
import com.torodb.torod.db.backends.converters.array.NullToArrayConverter;
import com.torodb.torod.db.backends.converters.array.StringToArrayConverter;
import com.torodb.torod.db.backends.converters.array.TimeToArrayConverter;
import com.torodb.torod.db.backends.converters.array.ValueToArrayConverterProvider;

/**
 *
 */
public class MySQLValueToArrayConverterProvider implements ValueToArrayConverterProvider {

    private static final long serialVersionUID = 1L;

    private final Map<ScalarType, ArrayConverter> converters;
    private final ArrayConverter<JsonArray, ScalarArray> arrayConverter;
    private final ArrayConverter<JsonValue, ScalarBoolean> booleanConverter;
    private final ArrayConverter<JsonString, ScalarDate> dateConverter;
    private final ArrayConverter<JsonString, ScalarInstant> dateTimeConverter;
    private final DoubleToArrayConverter doubleConverter;
    private final ArrayConverter<JsonNumber, ScalarInteger> integerConverter;
    private final ArrayConverter<JsonNumber, ScalarLong> longConverter;
    private final ArrayConverter<JsonValue, ScalarNull> nullConverter;
    private final ArrayConverter<JsonString, ScalarString> stringConverter;
    private final ArrayConverter<JsonString, ScalarTime> timeConverter;
    private final ArrayConverter<JsonString, ScalarMongoObjectId> mongoObjectIdConverter;
    private final MongoTimestampToArrayConverter mongoTimestampConverter;
    private final ArrayConverter<JsonString, ScalarBinary> binaryConverter;

    private MySQLValueToArrayConverterProvider() {
        arrayConverter = new ArrayToArrayConverter(this);
        booleanConverter = new BooleanToArrayConverter();
        dateConverter = new DateToArrayConverter();
        dateTimeConverter = new InstantToArrayConverter();
        doubleConverter = new DoubleToArrayConverter();
        integerConverter = new IntegerToArrayConverter();
        longConverter = new LongToArrayConverter();
        nullConverter = new NullToArrayConverter();
        stringConverter = new StringToArrayConverter();
        timeConverter = new TimeToArrayConverter();
        mongoObjectIdConverter = new MongoObjectIdToArrayConverter();
        mongoTimestampConverter = new MongoTimestampToArrayConverter();
        binaryConverter = new BinaryToArrayConverter();

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

    public static MySQLValueToArrayConverterProvider getInstance() {
        return ToArrayConverterHolder.INSTANCE;
    }

    @Override
    public ArrayConverter getConverter(ScalarType valueType) {
        ArrayConverter converter = converters.get(valueType);
        if (converter == null) {
            throw new AssertionError("There is no converter that converts "
                    + "elements of type " + valueType);
        }
        return converter;
    }
    
    @Override
    public ArrayConverter fromJsonValue(JsonValue jsonValue) {
        switch (jsonValue.getValueType()) {
            case ARRAY:
                assert jsonValue instanceof JsonArray;
                return arrayConverter;
            case TRUE:
            case FALSE:
                return booleanConverter;
            case NULL:
                return nullConverter;
            case NUMBER:
                assert jsonValue instanceof JsonNumber;
                JsonNumber number = (JsonNumber) jsonValue;
                if (number.isIntegral()) {
                    try {
                        long l = number.longValueExact();
                        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                            return longConverter;
                        }
                        return integerConverter;
                    }
                    catch (ArithmeticException ex) {
                        throw new ToroImplementationException(
                                "Unexpected integral value. " + number + " is "
                                + "bigger than long values"
                        );
                    }
                }
                //TODO: mongo timestamp lose type in MySQL arrays
                //if (mongoTimestampConverter.isValid(asObject)) {
                //    return mongoTimestampConverter;
                //}
                return doubleConverter;
            case STRING:
                assert jsonValue instanceof JsonString;
                return stringConverter;
            case OBJECT: {
                throw new IllegalArgumentException("Te recived JsonObject " + jsonValue
                        + " was not recognized as a valid ScalarValue codification");
            }
            default:
                throw new IllegalArgumentException("Instances of '"
                        + jsonValue.getClass() + "' like '" + jsonValue
                        + "' are not supported");
        }
    }

    public ArrayConverter<JsonArray, ScalarArray> getArrayConverter() {
        return arrayConverter;
    }

    public ArrayConverter<JsonValue, ScalarBoolean> getBooleanConverter() {
        return booleanConverter;
    }

    public ArrayConverter<JsonString, ScalarDate> getDateConverter() {
        return dateConverter;
    }

    public ArrayConverter<JsonString, ScalarInstant> getInstantConverter() {
        return dateTimeConverter;
    }

    public DoubleToArrayConverter getDoubleConverter() {
        return doubleConverter;
    }

    public ArrayConverter<JsonNumber, ScalarInteger> getIntegerConverter() {
        return integerConverter;
    }

    public ArrayConverter<JsonNumber, ScalarLong> getLongConverter() {
        return longConverter;
    }

    public ArrayConverter<JsonValue, ScalarNull> getNullConverter() {
        return nullConverter;
    }

    public ArrayConverter<JsonString, ScalarString> getStringConverter() {
        return stringConverter;
    }

    public ArrayConverter<JsonString, ScalarTime> getTimeConverter() {
        return timeConverter;
    }

    public ArrayConverter<JsonString, ScalarMongoObjectId> getMongoObjectIdConverter() {
        return mongoObjectIdConverter;
    }
    
    public MongoTimestampToArrayConverter getMongoTimestampConverter() {
        return mongoTimestampConverter;
    }
    
    public ArrayConverter<JsonString, ScalarBinary> getBinaryConverter() {
        return binaryConverter;
    }

    private static class ToArrayConverterHolder {

        private static final MySQLValueToArrayConverterProvider INSTANCE
                = new MySQLValueToArrayConverterProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return MySQLValueToArrayConverterProvider.getInstance();
    }
}
