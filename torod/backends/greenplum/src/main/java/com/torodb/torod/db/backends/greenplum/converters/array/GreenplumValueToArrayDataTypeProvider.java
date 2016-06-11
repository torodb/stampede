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

package com.torodb.torod.db.backends.greenplum.converters.array;

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
import com.torodb.torod.db.backends.converters.array.ValueToArrayDataTypeProvider;
import com.torodb.torod.db.backends.converters.jooq.ArrayToJooqConverter;

/**
 *
 */
public class GreenplumValueToArrayDataTypeProvider implements ValueToArrayDataTypeProvider {

    private static final long serialVersionUID = 1L;

    private final Map<ScalarType, DataType<?>> converters;

    private GreenplumValueToArrayDataTypeProvider() {
        GreenplumValueToArrayConverterProvider greenplumValueToArrayConverterProvider = GreenplumValueToArrayConverterProvider.getInstance();
        
        converters = Maps.newEnumMap(ScalarType.class);
        converters.put(ScalarType.ARRAY, ArrayToJooqConverter.fromScalarValue(ScalarArray.class, greenplumValueToArrayConverterProvider.getArrayConverter(), "json"));
        converters.put(ScalarType.BOOLEAN, ArrayToJooqConverter.fromScalarValue(ScalarBoolean.class, greenplumValueToArrayConverterProvider.getBooleanConverter(), "json"));
        converters.put(ScalarType.DATE, ArrayToJooqConverter.fromScalarValue(ScalarDate.class, greenplumValueToArrayConverterProvider.getDateConverter(), "json"));
        converters.put(ScalarType.INSTANT, ArrayToJooqConverter.fromScalarValue(ScalarInstant.class, greenplumValueToArrayConverterProvider.getInstantConverter(), "json"));
        converters.put(ScalarType.DOUBLE, ArrayToJooqConverter.fromScalarValue(ScalarDouble.class, greenplumValueToArrayConverterProvider.getDoubleConverter(), "json"));
        converters.put(ScalarType.INTEGER, ArrayToJooqConverter.fromScalarValue(ScalarInteger.class, greenplumValueToArrayConverterProvider.getIntegerConverter(), "json"));
        converters.put(ScalarType.LONG, ArrayToJooqConverter.fromScalarValue(ScalarLong.class, greenplumValueToArrayConverterProvider.getLongConverter(), "json"));
        converters.put(ScalarType.NULL, ArrayToJooqConverter.fromScalarValue(ScalarNull.class, greenplumValueToArrayConverterProvider.getNullConverter(), "json"));
        converters.put(ScalarType.STRING, ArrayToJooqConverter.fromScalarValue(ScalarString.class, greenplumValueToArrayConverterProvider.getStringConverter(), "json"));
        converters.put(ScalarType.TIME, ArrayToJooqConverter.fromScalarValue(ScalarTime.class, greenplumValueToArrayConverterProvider.getTimeConverter(), "json"));
        converters.put(ScalarType.MONGO_OBJECT_ID, ArrayToJooqConverter.fromScalarValue(ScalarMongoObjectId.class, greenplumValueToArrayConverterProvider.getMongoObjectIdConverter(), "json"));
        converters.put(ScalarType.MONGO_TIMESTAMP, ArrayToJooqConverter.fromScalarValue(ScalarMongoTimestamp.class, greenplumValueToArrayConverterProvider.getMongoTimestampConverter(), "json"));
        converters.put(ScalarType.BINARY, ArrayToJooqConverter.fromScalarValue(ScalarBinary.class, greenplumValueToArrayConverterProvider.getBinaryConverter(), "json"));
    }

    public static GreenplumValueToArrayDataTypeProvider getInstance() {
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

        private static final GreenplumValueToArrayDataTypeProvider INSTANCE
                = new GreenplumValueToArrayDataTypeProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return GreenplumValueToArrayDataTypeProvider.getInstance();
    }
}
