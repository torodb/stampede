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

package com.torodb.backend.derby;

import javax.inject.Singleton;

import org.jooq.SQLDialect;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.AbstractDataTypeProvider;
import com.torodb.backend.converters.jooq.DataTypeForKV;
import com.torodb.backend.derby.converters.jooq.BinaryValueConverter;
import com.torodb.backend.derby.converters.jooq.BooleanValueConverter;
import com.torodb.backend.derby.converters.jooq.DateValueConverter;
import com.torodb.backend.derby.converters.jooq.DoubleValueConverter;
import com.torodb.backend.derby.converters.jooq.InstantValueConverter;
import com.torodb.backend.derby.converters.jooq.IntegerValueConverter;
import com.torodb.backend.derby.converters.jooq.LongValueConverter;
import com.torodb.backend.derby.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.backend.derby.converters.jooq.MongoTimestampValueConverter;
import com.torodb.backend.derby.converters.jooq.NullValueConverter;
import com.torodb.backend.derby.converters.jooq.StringValueConverter;
import com.torodb.backend.derby.converters.jooq.TimeValueConverter;
import com.torodb.core.transaction.metainf.FieldType;

/**
 *
 */
@Singleton
public class DerbyDataTypeProvider extends AbstractDataTypeProvider {

    public DerbyDataTypeProvider() {
        super(
            ImmutableMap.<FieldType, DataTypeForKV<?>>builder()
                .put(FieldType.CHILD, BooleanValueConverter.TYPE)
                .put(FieldType.BOOLEAN, BooleanValueConverter.TYPE)
                .put(FieldType.DOUBLE, DoubleValueConverter.TYPE)
                .put(FieldType.INTEGER, IntegerValueConverter.TYPE)
                .put(FieldType.LONG, LongValueConverter.TYPE)
                .put(FieldType.NULL, NullValueConverter.TYPE)
                .put(FieldType.STRING, StringValueConverter.TYPE)
                .put(FieldType.DATE, DateValueConverter.TYPE)
                .put(FieldType.INSTANT, InstantValueConverter.TYPE)
                .put(FieldType.TIME, TimeValueConverter.TYPE)
                .put(FieldType.MONGO_OBJECT_ID, MongoObjectIdValueConverter.TYPE)
                .put(FieldType.MONGO_TIME_STAMP, MongoTimestampValueConverter.TYPE)
                .put(FieldType.BINARY, BinaryValueConverter.TYPE)
                .build()
        );
    }

    @Override
    public SQLDialect getDialect() {
        return SQLDialect.DERBY;
    }
}
