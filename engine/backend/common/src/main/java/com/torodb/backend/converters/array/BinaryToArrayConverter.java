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

package com.torodb.backend.converters.array;

import com.google.common.io.ByteSource;
import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.KvBinary.KvBinarySubtype;
import com.torodb.kvdocument.values.heap.ByteSourceKvBinary;

import javax.json.JsonString;

/**
 *
 */
public class BinaryToArrayConverter implements ArrayConverter<JsonString, KvBinary> {

  private static final long serialVersionUID = 1L;

  @Override
  public String toJsonLiteral(KvBinary value) {
    return value.toString();
  }

  @Override
  public KvBinary fromJsonValue(JsonString value) {
    byte[] bytes = HexUtils.hex2Bytes(value.getString());
    return new ByteSourceKvBinary(
        KvBinarySubtype.MONGO_GENERIC,
        (byte) 0,
        ByteSource.wrap(bytes)
    );
  }
}
