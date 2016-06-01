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

package com.torodb.torod.db.backends.postgresql.converters.jooq;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.core.subdocument.values.ScalarValue;
import com.torodb.torod.db.backends.converters.jooq.BinaryValueConverter;
import com.torodb.torod.db.backends.converters.jooq.BooleanValueConverter;
import com.torodb.torod.db.backends.converters.jooq.DateValueConverter;
import com.torodb.torod.db.backends.converters.jooq.DoubleValueConverter;
import com.torodb.torod.db.backends.converters.jooq.InstantValueConverter;
import com.torodb.torod.db.backends.converters.jooq.IntegerValueConverter;
import com.torodb.torod.db.backends.converters.jooq.LongValueConverter;
import com.torodb.torod.db.backends.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.torod.db.backends.converters.jooq.MongoTimestampValueConverter;
import com.torodb.torod.db.backends.converters.jooq.NullValueConverter;
import com.torodb.torod.db.backends.converters.jooq.StringValueConverter;
import com.torodb.torod.db.backends.converters.jooq.SubdocValueConverter;
import com.torodb.torod.db.backends.converters.jooq.TimeValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqConverterProvider;
import com.torodb.torod.db.backends.postgresql.converters.array.PostgreSQLValueToArrayConverterProvider;

/**
 *
 */
public class PostgreSQLValueToJooqConverterProvider implements ValueToJooqConverterProvider {

    private static final long serialVersionUID = 1L;

    /**
     * Types that are not supported.
     */
    private static final EnumSet<ScalarType> UNSUPPORTED_TYPES
            = EnumSet.noneOf(ScalarType.class);
    /**
     * Types that must be supported.
     */
    private static final Set<ScalarType> SUPPORTED_TYPES
            = Sets.difference(EnumSet.allOf(ScalarType.class), UNSUPPORTED_TYPES);
    private static final EnumMap<ScalarType, SubdocValueConverter> converters;

    static {
        converters = new EnumMap<>(ScalarType.class);

        converters.put(ScalarType.ARRAY, new ArrayValueConverter(PostgreSQLValueToArrayConverterProvider.getInstance()));
        converters.put(ScalarType.BOOLEAN, new BooleanValueConverter());
        converters.put(ScalarType.DOUBLE, new DoubleValueConverter());
        converters.put(ScalarType.INTEGER, new IntegerValueConverter());
        converters.put(ScalarType.LONG, new LongValueConverter());
        converters.put(ScalarType.NULL, new NullValueConverter());
        converters.put(ScalarType.STRING, new StringValueConverter());
        converters.put(ScalarType.DATE, new DateValueConverter());
        converters.put(ScalarType.INSTANT, new InstantValueConverter());
        converters.put(ScalarType.TIME, new TimeValueConverter());
        converters.put(ScalarType.MONGO_OBJECT_ID, new MongoObjectIdValueConverter());
        converters.put(ScalarType.MONGO_TIMESTAMP, new MongoTimestampValueConverter());
        converters.put(ScalarType.BINARY, new BinaryValueConverter());

        SetView<ScalarType> withoutConverter = Sets.difference(converters.keySet(), SUPPORTED_TYPES);
        if (!withoutConverter.isEmpty()) {
            throw new AssertionError("It is not defined how to convert from the following scalar "
                    + "types to json: " + withoutConverter);
        }
    }

    public SubdocValueConverter<?, ? extends ScalarValue<? extends Serializable>> getConverter(ScalarType type) {
        SubdocValueConverter converter = converters.get(type);
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
