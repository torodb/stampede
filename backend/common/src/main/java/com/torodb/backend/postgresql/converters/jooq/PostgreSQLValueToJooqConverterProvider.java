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

package com.torodb.backend.postgresql.converters.jooq;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.torodb.backend.converters.jooq.BinaryValueConverter;
import com.torodb.backend.converters.jooq.BooleanValueConverter;
import com.torodb.backend.converters.jooq.DateValueConverter;
import com.torodb.backend.converters.jooq.DoubleValueConverter;
import com.torodb.backend.converters.jooq.InstantValueConverter;
import com.torodb.backend.converters.jooq.IntegerValueConverter;
import com.torodb.backend.converters.jooq.KVValueConverter;
import com.torodb.backend.converters.jooq.LongValueConverter;
import com.torodb.backend.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.backend.converters.jooq.MongoTimestampValueConverter;
import com.torodb.backend.converters.jooq.NullValueConverter;
import com.torodb.backend.converters.jooq.StringValueConverter;
import com.torodb.backend.converters.jooq.TimeValueConverter;
import com.torodb.backend.converters.jooq.ValueToJooqConverterProvider;
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
import com.torodb.kvdocument.values.KVValue;

/**
 *
 */
public class PostgreSQLValueToJooqConverterProvider implements ValueToJooqConverterProvider {

    private static final long serialVersionUID = 1L;

    /**
     * Types that must be supported.
     */
    private static final Map<KVType, KVValueConverter> converters;

    static {
        converters = Maps.newHashMap();

        converters.put(BooleanType.INSTANCE, new BooleanValueConverter());
        converters.put(DoubleType.INSTANCE, new DoubleValueConverter());
        converters.put(IntegerType.INSTANCE, new IntegerValueConverter());
        converters.put(LongType.INSTANCE, new LongValueConverter());
        converters.put(NullType.INSTANCE, new NullValueConverter());
        converters.put(StringType.INSTANCE, new StringValueConverter());
        converters.put(DateType.INSTANCE, new DateValueConverter());
        converters.put(InstantType.INSTANCE, new InstantValueConverter());
        converters.put(TimeType.INSTANCE, new TimeValueConverter());
        converters.put(MongoObjectIdType.INSTANCE, new MongoObjectIdValueConverter());
        converters.put(MongoTimestampType.INSTANCE, new MongoTimestampValueConverter());
        converters.put(BinaryType.INSTANCE, new BinaryValueConverter());
    }

    public KVValueConverter<?, ? extends KVValue<? extends Serializable>> getConverter(KVType type) {
        KVValueConverter converter = converters.get(type);
        if (converter == null) {
            throw new IllegalArgumentException("It is not defined how to map elements of type " + type + " to SQL");
        }
        return converter;
    }

    public static PostgreSQLValueToJooqConverterProvider getInstance() {
        return ValueToJooqConverterProviderHolder.INSTANCE;
    }

    private static class ValueToJooqConverterProviderHolder {

        private static final PostgreSQLValueToJooqConverterProvider INSTANCE
                = new PostgreSQLValueToJooqConverterProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return PostgreSQLValueToJooqConverterProvider.getInstance();
    }
}
