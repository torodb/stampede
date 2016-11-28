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

import com.google.common.base.Preconditions;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KvMongoObjectId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 *
 */
public class ByteArrayKvMongoObjectId extends KvMongoObjectId {

  private static final long serialVersionUID = 2768008877077533010L;

  private final byte[] value;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
      justification = "We know this can be dangerous, but it improves the efficiency and, by"
      + "contract, the iterable shall be immutable")
  public ByteArrayKvMongoObjectId(@NotMutable byte[] value) {
    Preconditions.checkArgument(value.length == 12);
    this.value = value;
  }

  @Override
  public byte[] getArrayValue() {
    return Arrays.copyOf(value, value.length);
  }

  @Override
  protected byte[] getBytesUnsafe() {
    return value;
  }
}
