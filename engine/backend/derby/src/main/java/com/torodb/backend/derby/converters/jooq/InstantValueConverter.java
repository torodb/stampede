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
import com.torodb.backend.converters.sql.TimestampSqlBinding;
import com.torodb.backend.derby.converters.jooq.binding.TimestampBinding;
import com.torodb.kvdocument.types.InstantType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvInstant;
import com.torodb.kvdocument.values.heap.InstantKvInstant;

import java.sql.Timestamp;
import java.time.Instant;

public class InstantValueConverter implements KvValueConverter<Timestamp, Timestamp, KvInstant> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvInstant> TYPE = TimestampBinding.fromKvValue(KvInstant.class,
      new InstantValueConverter());

  @Override
  public KvType getErasuredType() {
    return InstantType.INSTANCE;
  }

  @Override
  public KvInstant from(Timestamp databaseObject) {
    return new InstantKvInstant(
        Instant.ofEpochSecond(databaseObject.getTime() / 1000, databaseObject.getNanos())
    );
  }

  @Override
  public Timestamp to(KvInstant userObject) {
    Instant instant = userObject.getValue();
    try {
      Timestamp ts = new Timestamp(instant.getEpochSecond() * 1000);
      ts.setNanos(instant.getNano());
      return ts;
    } catch (ArithmeticException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  @Override
  public Class<Timestamp> fromType() {
    return Timestamp.class;
  }

  @Override
  public Class<KvInstant> toType() {
    return KvInstant.class;
  }

  @Override
  public SqlBinding<Timestamp> getSqlBinding() {
    return TimestampSqlBinding.INSTANCE;
  }

}
