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

package com.torodb.poc.backend.postgresql.converters.json;

import java.util.Map;

import javax.annotation.Nonnull;

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
import com.torodb.poc.backend.converters.ValueConverter;
import com.torodb.poc.backend.converters.json.BinaryValueToJsonConverter;
import com.torodb.poc.backend.converters.json.BooleanValueToJsonConverter;
import com.torodb.poc.backend.converters.json.DateValueToJsonConverter;
import com.torodb.poc.backend.converters.json.DoubleValueToJsonConverter;
import com.torodb.poc.backend.converters.json.InstantValueToJsonConverter;
import com.torodb.poc.backend.converters.json.IntegerValueToJsonConverter;
import com.torodb.poc.backend.converters.json.LongValueToJsonConverter;
import com.torodb.poc.backend.converters.json.MongoObjectIdValueToJsonConverter;
import com.torodb.poc.backend.converters.json.MongoTimestampValueToJsonConverter;
import com.torodb.poc.backend.converters.json.NullValueToJsonConverter;
import com.torodb.poc.backend.converters.json.StringValueToJsonConverter;
import com.torodb.poc.backend.converters.json.TimeValueToJsonConverter;
import com.torodb.poc.backend.converters.json.ValueToJsonConverterProvider;
import com.torodb.poc.backend.mocks.ArrayTypeInstance;
import com.torodb.poc.backend.postgresql.converters.array.PostgreSQLValueToArrayConverterProvider;

/**
 *
 */
public class PostgreSQLValueToJsonConverterProvider implements ValueToJsonConverterProvider {

    private static final long serialVersionUID = 1L;

    private final Map<KVType, ValueConverter> converters;

    private PostgreSQLValueToJsonConverterProvider() {
        converters = Maps.newHashMap();
        converters.put(ArrayTypeInstance.GENERIC, new ArrayValueToJsonConverter(PostgreSQLValueToArrayConverterProvider.getInstance()));
        converters.put(BooleanType.INSTANCE, new BooleanValueToJsonConverter());
        converters.put(DateType.INSTANCE, new DateValueToJsonConverter());
        converters.put(InstantType.INSTANCE, new InstantValueToJsonConverter());
        converters.put(DoubleType.INSTANCE, new DoubleValueToJsonConverter());
        converters.put(IntegerType.INSTANCE, new IntegerValueToJsonConverter());
        converters.put(LongType.INSTANCE, new LongValueToJsonConverter());
        converters.put(NullType.INSTANCE, new NullValueToJsonConverter());
        converters.put(StringType.INSTANCE, new StringValueToJsonConverter());
        converters.put(TimeType.INSTANCE, new TimeValueToJsonConverter());
        converters.put(MongoObjectIdType.INSTANCE, new MongoObjectIdValueToJsonConverter());
        converters.put(MongoTimestampType.INSTANCE, new MongoTimestampValueToJsonConverter());
        converters.put(BinaryType.INSTANCE, new BinaryValueToJsonConverter());
    }

    public static PostgreSQLValueToJsonConverterProvider getInstance() {
        return ValueToJsonConverterProviderHolder.INSTANCE;
    }

    @Nonnull
    @Override
    public ValueConverter getConverter(KVType valueType) {
        ValueConverter converter = converters.get(valueType);
        if (converter == null) {
            throw new AssertionError("There is no converter that converts "
                    + "elements of type " + valueType);
        }
        return converter;
    }

    private static class ValueToJsonConverterProviderHolder {

        private static final PostgreSQLValueToJsonConverterProvider INSTANCE
                = new PostgreSQLValueToJsonConverterProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return PostgreSQLValueToJsonConverterProvider.getInstance();
    }
}
