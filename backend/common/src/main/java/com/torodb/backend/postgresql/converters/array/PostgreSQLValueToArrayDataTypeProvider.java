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

package com.torodb.backend.postgresql.converters.array;

import java.util.Map;

import javax.annotation.Nonnull;

import org.jooq.DataType;

import com.google.common.collect.Maps;
import com.torodb.backend.converters.array.ValueToArrayDataTypeProvider;
import com.torodb.backend.converters.jooq.binging.JSONBBinding;
import com.torodb.backend.mocks.ArrayTypeInstance;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KVType;
import com.torodb.kvdocument.types.LongType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.types.MongoTimestampType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.types.StringType;
import com.torodb.kvdocument.types.TimeType;
import com.torodb.kvdocument.values.KVArray;
import com.torodb.kvdocument.values.KVBinary;
import com.torodb.kvdocument.values.KVBoolean;
import com.torodb.kvdocument.values.KVDate;
import com.torodb.kvdocument.values.KVDouble;
import com.torodb.kvdocument.values.KVInstant;
import com.torodb.kvdocument.values.KVInteger;
import com.torodb.kvdocument.values.KVLong;
import com.torodb.kvdocument.values.KVMongoObjectId;
import com.torodb.kvdocument.values.KVMongoTimestamp;
import com.torodb.kvdocument.values.KVNull;
import com.torodb.kvdocument.values.KVString;
import com.torodb.kvdocument.values.KVTime;

/**
 *
 */
public class PostgreSQLValueToArrayDataTypeProvider implements ValueToArrayDataTypeProvider {

    private static final long serialVersionUID = 1L;

    private final Map<KVType, DataType<?>> converters;

    private PostgreSQLValueToArrayDataTypeProvider() {
        PostgreSQLValueToArrayConverterProvider postgreSQLValueToArrayConverterProvider = PostgreSQLValueToArrayConverterProvider.getInstance();
        
        converters = Maps.newHashMap();
        converters.put(ArrayTypeInstance.GENERIC, JSONBBinding.fromKVValue(KVArray.class, postgreSQLValueToArrayConverterProvider.getArrayConverter()));
        converters.put(BooleanType.INSTANCE, JSONBBinding.fromKVValue(KVBoolean.class, postgreSQLValueToArrayConverterProvider.getBooleanConverter()));
        converters.put(DateType.INSTANCE, JSONBBinding.fromKVValue(KVDate.class, postgreSQLValueToArrayConverterProvider.getDateConverter()));
        converters.put(InstantType.INSTANCE, JSONBBinding.fromKVValue(KVInstant.class, postgreSQLValueToArrayConverterProvider.getInstantConverter()));
        converters.put(DoubleType.INSTANCE, JSONBBinding.fromKVValue(KVDouble.class, postgreSQLValueToArrayConverterProvider.getDoubleConverter()));
        converters.put(IntegerType.INSTANCE, JSONBBinding.fromKVValue(KVInteger.class, postgreSQLValueToArrayConverterProvider.getIntegerConverter()));
        converters.put(LongType.INSTANCE, JSONBBinding.fromKVValue(KVLong.class, postgreSQLValueToArrayConverterProvider.getLongConverter()));
        converters.put(NullType.INSTANCE, JSONBBinding.fromKVValue(KVNull.class, postgreSQLValueToArrayConverterProvider.getNullConverter()));
        converters.put(StringType.INSTANCE, JSONBBinding.fromKVValue(KVString.class, postgreSQLValueToArrayConverterProvider.getStringConverter()));
        converters.put(TimeType.INSTANCE, JSONBBinding.fromKVValue(KVTime.class, postgreSQLValueToArrayConverterProvider.getTimeConverter()));
        converters.put(MongoObjectIdType.INSTANCE, JSONBBinding.fromKVValue(KVMongoObjectId.class, postgreSQLValueToArrayConverterProvider.getMongoObjectIdConverter()));
        converters.put(MongoTimestampType.INSTANCE, JSONBBinding.fromKVValue(KVMongoTimestamp.class, postgreSQLValueToArrayConverterProvider.getMongoTimestampConverter()));
        converters.put(BinaryType.INSTANCE, JSONBBinding.fromKVValue(KVBinary.class, postgreSQLValueToArrayConverterProvider.getBinaryConverter()));
    }

    public static PostgreSQLValueToArrayDataTypeProvider getInstance() {
        return ToArrayDataTypeHolder.INSTANCE;
    }

    @Nonnull
    public DataType<?> getDataType(KVType valueType) {
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
