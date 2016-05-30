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

package com.torodb.poc.backend.postgresql.converters.jooq;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
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
import com.torodb.poc.backend.converters.jooq.BinaryValueConverter;
import com.torodb.poc.backend.converters.jooq.BooleanValueConverter;
import com.torodb.poc.backend.converters.jooq.DataTypeForKV;
import com.torodb.poc.backend.converters.jooq.DateValueConverter;
import com.torodb.poc.backend.converters.jooq.DoubleValueConverter;
import com.torodb.poc.backend.converters.jooq.InstantValueConverter;
import com.torodb.poc.backend.converters.jooq.IntegerValueConverter;
import com.torodb.poc.backend.converters.jooq.LongValueConverter;
import com.torodb.poc.backend.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.poc.backend.converters.jooq.MongoTimestampValueConverter;
import com.torodb.poc.backend.converters.jooq.NullValueConverter;
import com.torodb.poc.backend.converters.jooq.StringValueConverter;
import com.torodb.poc.backend.converters.jooq.TimeValueConverter;
import com.torodb.poc.backend.converters.jooq.ValueToJooqDataTypeProvider;

/**
 *
 */
public class PostgreSQLValueToJooqDataTypeProvider implements ValueToJooqDataTypeProvider {

    private static final long serialVersionUID = 1L;

    private static final Map<KVType, DataTypeForKV<?>> dataTypes;

    static {
        dataTypes = Maps.newHashMap();

        dataTypes.put(BooleanType.INSTANCE, BooleanValueConverter.TYPE);
        dataTypes.put(DoubleType.INSTANCE, DoubleValueConverter.TYPE);
        dataTypes.put(IntegerType.INSTANCE, IntegerValueConverter.TYPE);
        dataTypes.put(LongType.INSTANCE, LongValueConverter.TYPE);
        dataTypes.put(NullType.INSTANCE, NullValueConverter.TYPE);
        dataTypes.put(StringType.INSTANCE, StringValueConverter.TYPE);
        dataTypes.put(DateType.INSTANCE, DateValueConverter.TYPE);
        dataTypes.put(InstantType.INSTANCE, InstantValueConverter.TYPE);
        dataTypes.put(TimeType.INSTANCE, TimeValueConverter.TYPE);
        dataTypes.put(MongoObjectIdType.INSTANCE, MongoObjectIdValueConverter.TYPE);
        dataTypes.put(MongoTimestampType.INSTANCE, MongoTimestampValueConverter.TYPE);
        dataTypes.put(BinaryType.INSTANCE, BinaryValueConverter.TYPE);
    }

    @Override
    public DataTypeForKV<?> getDataType(KVType type) {
        DataTypeForKV<?> dataType = dataTypes.get(type);
        if (dataType == null) {
            throw new IllegalArgumentException("It is not defined how to map elements of type " + type + " to SQL");
        }
        return dataType;
    }

    public static PostgreSQLValueToJooqDataTypeProvider getInstance() {
        return ValueToJooqDataTypeProviderHolder.INSTANCE;
    }

    private static class ValueToJooqDataTypeProviderHolder {

        private static final PostgreSQLValueToJooqDataTypeProvider INSTANCE
                = new PostgreSQLValueToJooqDataTypeProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return PostgreSQLValueToJooqDataTypeProvider.getInstance();
    }
}
