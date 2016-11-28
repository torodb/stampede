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
import com.torodb.backend.converters.sql.DoubleSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.DoubleType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvDouble;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

/**
 *
 */
public class DoubleValueConverter implements KvValueConverter<Double, Double, KvDouble> {

  private static final long serialVersionUID = 1L;

  public static final DataType<Double> DOUBLE_TYPE = new DefaultDataType<Double>(SQLDialect.DERBY,
      SQLDataType.DOUBLE, "DOUBLE");

  public static final DataTypeForKv<KvDouble> TYPE = DataTypeForKv.from(DOUBLE_TYPE,
      new DoubleValueConverter());

  @Override
  public KvType getErasuredType() {
    return DoubleType.INSTANCE;
  }

  @Override
  public KvDouble from(Double databaseObject) {
    return KvDouble.of(databaseObject);
  }

  @Override
  public Double to(KvDouble userObject) {
    return userObject.getValue();
  }

  @Override
  public Class<Double> fromType() {
    return Double.class;
  }

  @Override
  public Class<KvDouble> toType() {
    return KvDouble.class;
  }

  @Override
  public SqlBinding<Double> getSqlBinding() {
    return DoubleSqlBinding.INSTANCE;
  }

}
