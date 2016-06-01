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

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.torodb.torod.core.subdocument.ScalarType;
import com.torodb.torod.db.backends.converters.jooq.BinaryValueConverter;
import com.torodb.torod.db.backends.converters.jooq.BooleanValueConverter;
import com.torodb.torod.db.backends.converters.jooq.DataTypeForScalar;
import com.torodb.torod.db.backends.converters.jooq.DateValueConverter;
import com.torodb.torod.db.backends.converters.jooq.DoubleValueConverter;
import com.torodb.torod.db.backends.converters.jooq.InstantValueConverter;
import com.torodb.torod.db.backends.converters.jooq.IntegerValueConverter;
import com.torodb.torod.db.backends.converters.jooq.LongValueConverter;
import com.torodb.torod.db.backends.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.torod.db.backends.converters.jooq.MongoTimestampValueConverter;
import com.torodb.torod.db.backends.converters.jooq.NullValueConverter;
import com.torodb.torod.db.backends.converters.jooq.StringValueConverter;
import com.torodb.torod.db.backends.converters.jooq.TimeValueConverter;
import com.torodb.torod.db.backends.converters.jooq.ValueToJooqDataTypeProvider;

/**
 *
 */
public class PostgreSQLValueToJooqDataTypeProvider implements ValueToJooqDataTypeProvider {

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
    private static final EnumMap<ScalarType, DataTypeForScalar<?>> dataTypes;

    static {
        dataTypes = new EnumMap<>(ScalarType.class);

        dataTypes.put(ScalarType.ARRAY, ArrayValueConverter.TYPE);
        dataTypes.put(ScalarType.BOOLEAN, BooleanValueConverter.TYPE);
        dataTypes.put(ScalarType.DOUBLE, DoubleValueConverter.TYPE);
        dataTypes.put(ScalarType.INTEGER, IntegerValueConverter.TYPE);
        dataTypes.put(ScalarType.LONG, LongValueConverter.TYPE);
        dataTypes.put(ScalarType.NULL, NullValueConverter.TYPE);
        dataTypes.put(ScalarType.STRING, StringValueConverter.TYPE);
        dataTypes.put(ScalarType.DATE, DateValueConverter.TYPE);
        dataTypes.put(ScalarType.INSTANT, InstantValueConverter.TYPE);
        dataTypes.put(ScalarType.TIME, TimeValueConverter.TYPE);
        dataTypes.put(ScalarType.MONGO_OBJECT_ID, MongoObjectIdValueConverter.TYPE);
        dataTypes.put(ScalarType.MONGO_TIMESTAMP, MongoTimestampValueConverter.TYPE);
        dataTypes.put(ScalarType.BINARY, BinaryValueConverter.TYPE);

        SetView<ScalarType> withoutConverter = Sets.difference(dataTypes.keySet(), SUPPORTED_TYPES);
        if (!withoutConverter.isEmpty()) {
            throw new AssertionError("It is not defined how to convert from the following scalar "
                    + "types to json: " + withoutConverter);
        }
    }

    @Override
    public DataTypeForScalar<?> getDataType(ScalarType type) {
        DataTypeForScalar<?> dataType = dataTypes.get(type);
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
