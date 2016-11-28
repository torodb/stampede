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

package com.torodb.kvdocument.values.heap;

import com.google.common.io.ByteSource;
import com.torodb.kvdocument.values.KvBinary;
import com.torodb.kvdocument.values.utils.NonIoByteSource;

/**
 *
 */
public class ByteSourceKvBinary extends KvBinary {

  private static final long serialVersionUID = 1856319675637762928L;

  private final KvBinarySubtype subtype;
  private final NonIoByteSource byteSource;
  private final byte category;

  public ByteSourceKvBinary(KvBinarySubtype subtype, byte category, NonIoByteSource byteSource) {
    this.subtype = subtype;
    this.byteSource = byteSource;
    this.category = category;
  }

  public ByteSourceKvBinary(KvBinarySubtype subtype, byte category, ByteSource byteSource) {
    this(subtype, category, new NonIoByteSource(byteSource));
  }

  @Override
  public NonIoByteSource getByteSource() {
    return byteSource;
  }

  @Override
  public KvBinarySubtype getSubtype() {
    return subtype;
  }

  @Override
  public byte getCategory() {
    return category;
  }

}
