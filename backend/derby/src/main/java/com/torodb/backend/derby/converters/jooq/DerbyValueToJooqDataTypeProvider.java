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

package com.torodb.backend.derby.converters.jooq;

import java.util.Map;

import com.google.common.collect.Maps;
import com.torodb.backend.converters.jooq.BooleanValueConverter;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.converters.jooq.DateValueConverter;
import com.torodb.backend.converters.jooq.DoubleValueConverter;
import com.torodb.backend.converters.jooq.IntegerValueConverter;
import com.torodb.backend.converters.jooq.LongValueConverter;
import com.torodb.backend.converters.jooq.NullValueConverter;
import com.torodb.backend.converters.jooq.TimeValueConverter;
import com.torodb.backend.converters.jooq.ValueToJooqDataTypeProvider;
import com.torodb.core.transaction.metainf.FieldType;

/**
 *
 */
public class DerbyValueToJooqDataTypeProvider implements ValueToJooqDataTypeProvider {

    private static final long serialVersionUID = 1L;

    private static final Map<FieldType, DataTypeForKV<?>> dataTypes;

    static {
        dataTypes = Maps.newHashMap();

        dataTypes.put(FieldType.BOOLEAN, BooleanValueConverter.TYPE);
        dataTypes.put(FieldType.CHILD, BooleanValueConverter.TYPE);
        dataTypes.put(FieldType.DOUBLE, DoubleValueConverter.TYPE);
        dataTypes.put(FieldType.INTEGER, IntegerValueConverter.TYPE);
        dataTypes.put(FieldType.LONG, LongValueConverter.TYPE);
        dataTypes.put(FieldType.NULL, NullValueConverter.TYPE);
        dataTypes.put(FieldType.STRING, StringValueConverter.TYPE);
        dataTypes.put(FieldType.DATE, DateValueConverter.TYPE);
        dataTypes.put(FieldType.INSTANT, InstantValueConverter.TYPE);
        dataTypes.put(FieldType.TIME, TimeValueConverter.TYPE);
        dataTypes.put(FieldType.MONGO_OBJECT_ID, MongoObjectIdValueConverter.TYPE);
        dataTypes.put(FieldType.MONGO_TIME_STAMP, MongoTimestampValueConverter.TYPE);
        dataTypes.put(FieldType.BINARY, BinaryValueConverter.TYPE);
    }

    @Override
    public DataTypeForKV<?> getDataType(FieldType type) {
        DataTypeForKV<?> dataType = dataTypes.get(type);
        if (dataType == null) {
            throw new IllegalArgumentException("It is not defined how to map elements of type " + type + " to SQL");
        }
        return dataType;
    }

    public static DerbyValueToJooqDataTypeProvider getInstance() {
        return ValueToJooqDataTypeProviderHolder.INSTANCE;
    }

    private static class ValueToJooqDataTypeProviderHolder {

        private static final DerbyValueToJooqDataTypeProvider INSTANCE
                = new DerbyValueToJooqDataTypeProvider();
    }

    //@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD")
    private Object readResolve() {
        return DerbyValueToJooqDataTypeProvider.getInstance();
    }
}
