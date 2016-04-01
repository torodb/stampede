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

import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.DataType;

import com.google.common.collect.Maps;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarArray;
import com.torodb.torod.core.subdocument.values.ScalarBinary;
import com.torodb.torod.core.subdocument.values.ScalarBoolean;
import com.torodb.torod.core.subdocument.values.ScalarDate;
import com.torodb.torod.core.subdocument.values.ScalarDouble;
import com.torodb.torod.core.subdocument.values.ScalarInstant;
import com.torodb.torod.core.subdocument.values.ScalarInteger;
import com.torodb.torod.core.subdocument.values.ScalarLong;
import com.torodb.torod.core.subdocument.values.ScalarMongoObjectId;
import com.torodb.torod.core.subdocument.values.ScalarMongoTimestamp;
import com.torodb.torod.core.subdocument.values.ScalarNull;
import com.torodb.torod.core.subdocument.values.ScalarString;
import com.torodb.torod.core.subdocument.values.ScalarTime;
import com.torodb.torod.db.backends.converters.jooq.JSONBBinding;
import com.torodb.torod.db.backends.converters.json.DoubleValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.InstantValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.IntegerValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.LongValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.MongoTimestampValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.NullValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.StringValueToJsonConverter;
import com.torodb.torod.db.backends.converters.json.TimeValueToJsonConverter;

/**
 *
 */
public class ValueToArrayDataTypeProvider {

    private final Map<ScalarType, DataType<?>> converters;

    private ValueToArrayDataTypeProvider() {
        ValueToArrayConverterProvider valueToArrayConverterProvider = ValueToArrayConverterProvider.getInstance();
        
        converters = Maps.newEnumMap(ScalarType.class);
        converters.put(ScalarType.ARRAY, JSONBBinding.fromScalarValue(ScalarArray.class, valueToArrayConverterProvider.getArrayConverter()));
        converters.put(ScalarType.BOOLEAN, JSONBBinding.fromScalarValue(ScalarBoolean.class, valueToArrayConverterProvider.getBooleanConverter()));
        converters.put(ScalarType.DATE, JSONBBinding.fromScalarValue(ScalarDate.class, valueToArrayConverterProvider.getDateConverter()));
        converters.put(ScalarType.INSTANT, JSONBBinding.fromScalarValue(ScalarInstant.class, valueToArrayConverterProvider.getInstantConverter()));
        converters.put(ScalarType.DOUBLE, JSONBBinding.fromScalarValue(ScalarDouble.class, valueToArrayConverterProvider.getDoubleConverter()));
        converters.put(ScalarType.INTEGER, JSONBBinding.fromScalarValue(ScalarInteger.class, valueToArrayConverterProvider.getIntegerConverter()));
        converters.put(ScalarType.LONG, JSONBBinding.fromScalarValue(ScalarLong.class, valueToArrayConverterProvider.getLongConverter()));
        converters.put(ScalarType.NULL, JSONBBinding.fromScalarValue(ScalarNull.class, valueToArrayConverterProvider.getNullConverter()));
        converters.put(ScalarType.STRING, JSONBBinding.fromScalarValue(ScalarString.class, valueToArrayConverterProvider.getStringConverter()));
        converters.put(ScalarType.TIME, JSONBBinding.fromScalarValue(ScalarTime.class, valueToArrayConverterProvider.getTimeConverter()));
        converters.put(ScalarType.MONGO_OBJECT_ID, JSONBBinding.fromScalarValue(ScalarMongoObjectId.class, valueToArrayConverterProvider.getMongoObjectIdConverter()));
        converters.put(ScalarType.MONGO_TIMESTAMP, JSONBBinding.fromScalarValue(ScalarMongoTimestamp.class, valueToArrayConverterProvider.getMongoTimestampConverter()));
        converters.put(ScalarType.BINARY, JSONBBinding.fromScalarValue(ScalarBinary.class, valueToArrayConverterProvider.getBinaryConverter()));
    }

    public static ValueToArrayDataTypeProvider getInstance() {
        return ToArrayDataTypeHolder.INSTANCE;
    }

    @Nonnull
    public DataType<?> getDataType(ScalarType valueType) {
        DataType<?> converter = converters.get(valueType);
        if (converter == null) {
            throw new AssertionError("There is no data type for "
                    + "elements of type " + valueType);
        }
        return converter;
    }

    private static class ToArrayDataTypeHolder {

        private static final ValueToArrayDataTypeProvider INSTANCE
                = new ValueToArrayDataTypeProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return ValueToArrayDataTypeProvider.getInstance();
    }
}
