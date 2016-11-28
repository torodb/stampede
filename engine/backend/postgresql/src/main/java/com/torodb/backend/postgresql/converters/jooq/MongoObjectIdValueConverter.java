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
import com.torodb.backend.converters.sql.BinarySqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.types.MongoObjectIdType;
import com.torodb.kvdocument.values.KvMongoObjectId;
import com.torodb.kvdocument.values.heap.ByteArrayKvMongoObjectId;
import org.jooq.util.postgres.PostgresDataType;

import java.sql.Types;

/**
 *
 */
public class MongoObjectIdValueConverter implements
    KvValueConverter<byte[], byte[], KvMongoObjectId> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvMongoObjectId> TYPE = DataTypeForKv.from(
      PostgresDataType.BYTEA, new MongoObjectIdValueConverter(), Types.BINARY);

  @Override
  public KvType getErasuredType() {
    return MongoObjectIdType.INSTANCE;
  }

  @Override
  public KvMongoObjectId from(byte[] databaseObject) {
    return new ByteArrayKvMongoObjectId(databaseObject);
  }

  @Override
  public byte[] to(KvMongoObjectId userObject) {
    return userObject.getArrayValue();
  }

  @Override
  public Class<byte[]> fromType() {
    return byte[].class;
  }

  @Override
  public Class<KvMongoObjectId> toType() {
    return KvMongoObjectId.class;
  }

  @Override
  public SqlBinding<byte[]> getSqlBinding() {
    return BinarySqlBinding.INSTANCE;
  }

}
