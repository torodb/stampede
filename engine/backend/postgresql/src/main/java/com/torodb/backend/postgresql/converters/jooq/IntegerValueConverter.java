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
import com.torodb.backend.converters.sql.IntegerSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.IntegerType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvInteger;
import org.jooq.util.postgres.PostgresDataType;

/**
 *
 */
public class IntegerValueConverter implements KvValueConverter<Integer, Integer, KvInteger> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvInteger> TYPE = DataTypeForKv.from(PostgresDataType.INT4,
      new IntegerValueConverter());

  @Override
  public KvType getErasuredType() {
    return IntegerType.INSTANCE;
  }

  @Override
  public KvInteger from(Integer databaseObject) {
    return KvInteger.of(databaseObject);
  }

  @Override
  public Integer to(KvInteger userObject) {
    return userObject.getValue();
  }

  @Override
  public Class<Integer> fromType() {
    return Integer.class;
  }

  @Override
  public Class<KvInteger> toType() {
    return KvInteger.class;
  }

  @Override
  public SqlBinding<Integer> getSqlBinding() {
    return IntegerSqlBinding.INSTANCE;
  }
}
