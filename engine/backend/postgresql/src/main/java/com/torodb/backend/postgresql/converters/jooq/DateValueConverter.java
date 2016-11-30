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

package com.torodb.backend.postgresql.converters.jooq;

import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.DateSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.DateType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvDate;
import com.torodb.kvdocument.values.heap.LocalDateKvDate;
import org.jooq.impl.SQLDataType;

import java.sql.Date;

/**
 *
 */
public class DateValueConverter implements KvValueConverter<Date, Date, KvDate> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvDate> TYPE = DataTypeForKv.from(SQLDataType.DATE,
      new DateValueConverter());

  @Override
  public KvType getErasuredType() {
    return DateType.INSTANCE;
  }

  @Override
  public KvDate from(Date databaseObject) {
    return new LocalDateKvDate(databaseObject.toLocalDate());
  }

  @Override
  public Date to(KvDate userObject) {
    return Date.valueOf(userObject.getValue());
  }

  @Override
  public Class<Date> fromType() {
    return Date.class;
  }

  @Override
  public Class<KvDate> toType() {
    return KvDate.class;
  }

  @Override
  public SqlBinding<Date> getSqlBinding() {
    return DateSqlBinding.INSTANCE;
  }

}
