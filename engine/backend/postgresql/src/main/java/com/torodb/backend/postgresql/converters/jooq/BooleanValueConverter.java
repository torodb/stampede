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
import com.torodb.backend.converters.sql.BooleanSqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.BooleanType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvBoolean;
import org.jooq.util.postgres.PostgresDataType;

import java.sql.Types;

/**
 *
 */
public class BooleanValueConverter implements KvValueConverter<Boolean, Boolean, KvBoolean> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvBoolean> TYPE = DataTypeForKv.from(PostgresDataType.BOOL,
      new BooleanValueConverter(), Types.BIT);

  @Override
  public KvType getErasuredType() {
    return BooleanType.INSTANCE;
  }

  @Override
  public KvBoolean from(Boolean databaseObject) {
    return KvBoolean.from(databaseObject);
  }

  @Override
  public Boolean to(KvBoolean userObject) {
    return userObject.getValue();
  }

  @Override
  public Class<Boolean> fromType() {
    return Boolean.class;
  }

  @Override
  public Class<KvBoolean> toType() {
    return KvBoolean.class;
  }

  @Override
  public SqlBinding<Boolean> getSqlBinding() {
    return BooleanSqlBinding.INSTANCE;
  }

}
