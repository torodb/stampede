/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.postgresql;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.AbstractDataTypeProvider;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.postgresql.converters.jooq.BinaryValueConverter;
import com.torodb.backend.postgresql.converters.jooq.BooleanValueConverter;
import com.torodb.backend.postgresql.converters.jooq.DateValueConverter;
import com.torodb.backend.postgresql.converters.jooq.DoubleValueConverter;
import com.torodb.backend.postgresql.converters.jooq.InstantValueConverter;
import com.torodb.backend.postgresql.converters.jooq.IntegerValueConverter;
import com.torodb.backend.postgresql.converters.jooq.LongValueConverter;
import com.torodb.backend.postgresql.converters.jooq.MongoObjectIdValueConverter;
import com.torodb.backend.postgresql.converters.jooq.MongoTimestampValueConverter;
import com.torodb.backend.postgresql.converters.jooq.NullValueConverter;
import com.torodb.backend.postgresql.converters.jooq.StringValueConverter;
import com.torodb.backend.postgresql.converters.jooq.TimeValueConverter;
import com.torodb.core.transaction.metainf.FieldType;
import org.jooq.SQLDialect;

import javax.inject.Singleton;

@Singleton
public class PostgreSqlDataTypeProvider extends AbstractDataTypeProvider {

  public PostgreSqlDataTypeProvider() {
    super(ImmutableMap.<FieldType, DataTypeForKv<?>>builder()
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
    return SQLDialect.POSTGRES_9_4;
  }
}
