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
import com.torodb.backend.converters.sql.Decimal128SqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.Decimal128Type;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvDecimal128;

import org.jooq.util.postgres.PostgresDataType;

import java.math.BigDecimal;
import java.sql.Types;

public class Decimal128ValueConverter
    implements KvValueConverter<BigDecimal, BigDecimal, KvDecimal128> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvDecimal128> TYPE =
      DataTypeForKv.from(PostgresDataType.NUMERIC, new Decimal128ValueConverter(), Types.NUMERIC);

  @Override
  public KvType getErasuredType() {
    return Decimal128Type.INSTANCE;
  }

  @Override
  public KvDecimal128 from(BigDecimal value) {
    return KvDecimal128.of(value);
  }

  @Override
  public BigDecimal to(KvDecimal128 userObject) {
    return userObject.getValue();
  }

  @Override
  public Class<BigDecimal> fromType() {
    return BigDecimal.class;
  }

  @Override
  public Class<KvDecimal128> toType() {
    return KvDecimal128.class;
  }

  @Override
  public SqlBinding<BigDecimal> getSqlBinding() {
    return Decimal128SqlBinding.INSTANCE;
  }

}
