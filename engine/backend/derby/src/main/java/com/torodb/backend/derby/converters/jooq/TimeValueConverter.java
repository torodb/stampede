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

package com.torodb.backend.derby.converters.jooq;

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.backend.converters.sql.TimeSqlBinding;
import com.torodb.backend.derby.converters.jooq.binding.TimeBinding;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.TimeType;
import com.torodb.kvdocument.values.KvTime;
import com.torodb.kvdocument.values.heap.LocalTimeKvTime;

import java.sql.Time;

/**
 *
 */
public class TimeValueConverter implements KvValueConverter<Time, Time, KvTime> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvTime> TYPE = TimeBinding.fromKvValue(KvTime.class,
      new TimeValueConverter());

  @Override
  public KvType getErasuredType() {
    return TimeType.INSTANCE;
  }

  @Override
  public KvTime from(Time databaseObject) {
    return new LocalTimeKvTime(databaseObject.toLocalTime());
  }

  @Override
  public Time to(KvTime userObject) {
    return Time.valueOf(userObject.getValue());
  }

  @Override
  public Class<Time> fromType() {
    return Time.class;
  }

  @Override
  public Class<KvTime> toType() {
    return KvTime.class;
  }

  @Override
  public SqlBinding<Time> getSqlBinding() {
    return TimeSqlBinding.INSTANCE;
  }

}
