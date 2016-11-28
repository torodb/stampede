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

import com.google.common.primitives.Doubles;
import com.torodb.kvdocument.types.DoubleType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public abstract class KvDouble extends KvNumeric<Double> {

  private static final long serialVersionUID = 6351251976353558479L;

  public static KvDouble of(double d) {
    if (d == 0) {
      return DefaultKvDouble.ZERO;
    }
    if (d == 1) {
      return DefaultKvDouble.ONE;
    }
    if (d == -1) {
      return DefaultKvDouble.MINUS_ONE;
    }
    return new DefaultKvDouble(d);
  }

  @Override
  public DoubleType getType() {
    return DoubleType.INSTANCE;
  }

  @Override
  public Double getValue() {
    return doubleValue();
  }

  @Override
  public int intValue() {
    return (int) doubleValue();
  }

  @Override
  public long longValue() {
    return (long) doubleValue();
  }

  @Override
  public Class<? extends Double> getValueClass() {
    return Double.class;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  @Override
  public int hashCode() {
    return Doubles.hashCode(doubleValue());
  }

  @SuppressFBWarnings(value = "FE_FLOATING_POINT_EQUALITY",
      justification = "We want to check for exactly equality")
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvDouble)) {
      return false;
    }
    return this.doubleValue() == ((KvDouble) obj).doubleValue();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  private static class DefaultKvDouble extends KvDouble {

    private static final long serialVersionUID = -4945208796227558609L;
    private static final DefaultKvDouble ZERO = new DefaultKvDouble(0);
    private static final DefaultKvDouble ONE = new DefaultKvDouble(1);
    private static final DefaultKvDouble MINUS_ONE = new DefaultKvDouble(-1);
    private final double value;

    public DefaultKvDouble(double value) {
      this.value = value;
    }

    @Override
    public double doubleValue() {
      return value;
    }
  }
}
