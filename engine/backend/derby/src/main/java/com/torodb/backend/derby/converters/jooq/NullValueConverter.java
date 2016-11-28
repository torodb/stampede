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
import com.torodb.backend.converters.sql.BooleanSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.NullType;
import com.torodb.kvdocument.values.KvNull;

/**
 *
 */
public class NullValueConverter implements KvValueConverter<Boolean, Boolean, KvNull> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvNull> TYPE = DataTypeForKv.from(
      BooleanValueConverter.BOOLEAN_TYPE, new NullValueConverter());

  @Override
  public KvType getErasuredType() {
    return NullType.INSTANCE;
  }

  @Override
  public KvNull from(Boolean databaseObject) {
    return KvNull.getInstance();
  }

  @Override
  public Boolean to(KvNull userObject) {
    return Boolean.TRUE;
  }

  @Override
  public Class<Boolean> fromType() {
    return Boolean.class;
  }

  @Override
  public Class<KvNull> toType() {
    return KvNull.class;
  }

  @Override
  public SqlBinding<Boolean> getSqlBinding() {
    return BooleanSqlBinding.INSTANCE;
  }

}
