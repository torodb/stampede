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

package com.torodb.kvdocument.values;

import com.google.common.primitives.Longs;
import com.torodb.kvdocument.types.LongType;

public abstract class KvLong extends KvNumeric<Long> {

  private static final long serialVersionUID = -4342266574537851228L;

  public static KvLong of(long l) {
    if (l == 0) {
      return DefaultKvLong.ZERO;
    }
    if (l == 1) {
      return DefaultKvLong.ONE;
    }
    if (l == -1) {
      return DefaultKvLong.MINUS_ONE;
    }
    return new DefaultKvLong(l);
  }

  @Override
  public LongType getType() {
    return LongType.INSTANCE;
  }

  @Override
  public Long getValue() {
    return longValue();
  }

  @Override
  public int intValue() {
    return (int) longValue();
  }

  @Override
  public double doubleValue() {
    return longValue();
  }

  @Override
  public Class<? extends Long> getValueClass() {
    return Long.class;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(longValue());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvLong)) {
      return false;
    }
    return this.longValue() == ((KvLong) obj).longValue();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  private static class DefaultKvLong extends KvLong {

    private static final long serialVersionUID = 1412077251749154561L;

    private static final DefaultKvLong ZERO = new DefaultKvLong(0);
    private static final DefaultKvLong ONE = new DefaultKvLong(1);
    private static final DefaultKvLong MINUS_ONE = new DefaultKvLong(-1);

    private final long value;

    private DefaultKvLong(long value) {
      this.value = value;
    }

    @Override
    public long longValue() {
      return value;
    }

  }
}
