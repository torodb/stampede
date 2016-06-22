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

package com.torodb.backend.postgresql;

import javax.inject.Singleton;

import org.jooq.SQLDialect;

import com.torodb.backend.AbstractDataTypeProvider;
import com.torodb.backend.converters.jooq.BinaryValueConverter;
import com.torodb.backend.converters.jooq.BooleanValueConverter;
import com.torodb.backend.converters.jooq.DateValueConverter;
import com.torodb.backend.converters.jooq.DoubleValueConverter;
import com.torodb.backend.converters.jooq.InstantValueConverter;
import com.torodb.backend.converters.jooq.IntegerValueConverter;
import com.torodb.backend.converters.jooq.LongValueConverter;
import com.torodb.backend.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.backend.converters.jooq.MongoTimestampValueConverter;
import com.torodb.backend.converters.jooq.NullValueConverter;
import com.torodb.backend.converters.jooq.StringValueConverter;
import com.torodb.backend.converters.jooq.TimeValueConverter;

/**
 *
 */
@Singleton
public class PostgreSQLDataTypeProvider extends AbstractDataTypeProvider {

    public PostgreSQLDataTypeProvider() {
        super(
            BooleanValueConverter.TYPE,
            BooleanValueConverter.TYPE,
            DoubleValueConverter.TYPE,
            IntegerValueConverter.TYPE,
            LongValueConverter.TYPE,
            NullValueConverter.TYPE,
            StringValueConverter.TYPE,
            DateValueConverter.TYPE,
            InstantValueConverter.TYPE,
            TimeValueConverter.TYPE,
            MongoObjectIdValueConverter.TYPE,
            MongoTimestampValueConverter.TYPE,
            BinaryValueConverter.TYPE
        );
    }

    @Override
    public SQLDialect getDialect() {
        return SQLDialect.POSTGRES_9_4;
    }
}
