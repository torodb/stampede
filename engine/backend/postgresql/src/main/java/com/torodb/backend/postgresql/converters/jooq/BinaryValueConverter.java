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

import com.google.common.io.ByteSource;
import com.torodb.backend.converters.jooq.DataTypeForKv;
import com.torodb.backend.converters.jooq.KvValueConverter;
import com.torodb.backend.converters.sql.BinarySqlBinding;
import com.torodb.backend.converters.sql.SqlBinding;
import com.torodb.kvdocument.types.BinaryType;
import com.torodb.kvdocument.types.KvType;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBinary.KvBinarySubtype;
import com.torodb.kvdocument.values.heap.ByteSourceKvBinary;
import org.jooq.util.postgres.PostgresDataType;

/**
 *
 */
public class BinaryValueConverter implements
    KvValueConverter<byte[], byte[], KvBinary> {

  private static final long serialVersionUID = 1L;

  public static final DataTypeForKv<KvBinary> TYPE = DataTypeForKv.from(PostgresDataType.BYTEA,
      new BinaryValueConverter(), -2);

  @Override
  public KvType getErasuredType() {
    return BinaryType.INSTANCE;
  }

  @Override
  public KvBinary from(byte[] databaseObject) {
    return new ByteSourceKvBinary(KvBinarySubtype.MONGO_GENERIC, (byte) 0, ByteSource.wrap(
        databaseObject));
  }

  @Override
  public byte[] to(KvBinary userObject) {
    return userObject.getByteSource().read();
  }

  @Override
  public Class<byte[]> fromType() {
    return byte[].class;
  }

  @Override
  public Class<KvBinary> toType() {
    return KvBinary.class;
  }

  @Override
  public SqlBinding<byte[]> getSqlBinding() {
    return BinarySqlBinding.INSTANCE;
  }

}
