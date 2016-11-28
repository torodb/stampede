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

import com.torodb.kvdocument.types.IntegerType;

public abstract class KvInteger extends KvNumeric<Integer> {

  private static final long serialVersionUID = -8056479643235327356L;

  public static KvInteger of(int i) {
    if (i == 0) {
      return DefaultKvInteger.ZERO;
    }
    if (i == 1) {
      return DefaultKvInteger.ONE;
    }
    if (i == -1) {
      return DefaultKvInteger.MINUS_ONE;
    }
    return new DefaultKvInteger(i);
  }

  @Override
  public IntegerType getType() {
    return IntegerType.INSTANCE;
  }

  @Override
  public Integer getValue() {
    return intValue();
  }

  @Override
  public long longValue() {
    return intValue();
  }

  @Override
  public double doubleValue() {
    return intValue();
  }

  @Override
  public Class<? extends Integer> getValueClass() {
    return Integer.class;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public int hashCode() {
    return intValue();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvInteger)) {
      return false;
    }
    return this.intValue() == ((KvInteger) obj).intValue();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  private static class DefaultKvInteger extends KvInteger {

    private static final long serialVersionUID = 6292206125650070164L;
    private static final DefaultKvInteger ZERO = new DefaultKvInteger(0);
    private static final DefaultKvInteger ONE = new DefaultKvInteger(1);
    private static final DefaultKvInteger MINUS_ONE = new DefaultKvInteger(-1);

    private final int value;

    private DefaultKvInteger(int value) {
      this.value = value;
    }

    @Override
    public int intValue() {
      return value;
    }
  }
}
