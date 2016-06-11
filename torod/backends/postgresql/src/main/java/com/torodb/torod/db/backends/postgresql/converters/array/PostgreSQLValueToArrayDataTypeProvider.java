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

package com.torodb.torod.db.backends.postgresql.converters.array;

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
import com.torodb.torod.db.backends.converters.jooq.JSONBBinding;

/**
 *
 */
public class PostgreSQLValueToArrayDataTypeProvider implements ValueToArrayDataTypeProvider {

    private static final long serialVersionUID = 1L;

    private final Map<ScalarType, DataType<?>> converters;

    private PostgreSQLValueToArrayDataTypeProvider() {
        PostgreSQLValueToArrayConverterProvider postgreSQLValueToArrayConverterProvider = PostgreSQLValueToArrayConverterProvider.getInstance();
        
        converters = Maps.newEnumMap(ScalarType.class);
        converters.put(ScalarType.ARRAY, JSONBBinding.fromScalarValue(ScalarArray.class, postgreSQLValueToArrayConverterProvider.getArrayConverter()));
        converters.put(ScalarType.BOOLEAN, JSONBBinding.fromScalarValue(ScalarBoolean.class, postgreSQLValueToArrayConverterProvider.getBooleanConverter()));
        converters.put(ScalarType.DATE, JSONBBinding.fromScalarValue(ScalarDate.class, postgreSQLValueToArrayConverterProvider.getDateConverter()));
        converters.put(ScalarType.INSTANT, JSONBBinding.fromScalarValue(ScalarInstant.class, postgreSQLValueToArrayConverterProvider.getInstantConverter()));
        converters.put(ScalarType.DOUBLE, JSONBBinding.fromScalarValue(ScalarDouble.class, postgreSQLValueToArrayConverterProvider.getDoubleConverter()));
        converters.put(ScalarType.INTEGER, JSONBBinding.fromScalarValue(ScalarInteger.class, postgreSQLValueToArrayConverterProvider.getIntegerConverter()));
        converters.put(ScalarType.LONG, JSONBBinding.fromScalarValue(ScalarLong.class, postgreSQLValueToArrayConverterProvider.getLongConverter()));
        converters.put(ScalarType.NULL, JSONBBinding.fromScalarValue(ScalarNull.class, postgreSQLValueToArrayConverterProvider.getNullConverter()));
        converters.put(ScalarType.STRING, JSONBBinding.fromScalarValue(ScalarString.class, postgreSQLValueToArrayConverterProvider.getStringConverter()));
        converters.put(ScalarType.TIME, JSONBBinding.fromScalarValue(ScalarTime.class, postgreSQLValueToArrayConverterProvider.getTimeConverter()));
        converters.put(ScalarType.MONGO_OBJECT_ID, JSONBBinding.fromScalarValue(ScalarMongoObjectId.class, postgreSQLValueToArrayConverterProvider.getMongoObjectIdConverter()));
        converters.put(ScalarType.MONGO_TIMESTAMP, JSONBBinding.fromScalarValue(ScalarMongoTimestamp.class, postgreSQLValueToArrayConverterProvider.getMongoTimestampConverter()));
        converters.put(ScalarType.BINARY, JSONBBinding.fromScalarValue(ScalarBinary.class, postgreSQLValueToArrayConverterProvider.getBinaryConverter()));
    }

    public static PostgreSQLValueToArrayDataTypeProvider getInstance() {
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

        private static final PostgreSQLValueToArrayDataTypeProvider INSTANCE
                = new PostgreSQLValueToArrayDataTypeProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return PostgreSQLValueToArrayDataTypeProvider.getInstance();
    }
}
