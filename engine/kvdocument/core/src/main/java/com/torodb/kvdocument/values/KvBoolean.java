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

import com.google.common.primitives.Booleans;
import com.torodb.kvdocument.types.BooleanType;

public class KvBoolean extends KvValue<Boolean> {

  private static final long serialVersionUID = -89125370097900238L;

  public static final KvBoolean TRUE = new KvBoolean(true);
  public static final KvBoolean FALSE = new KvBoolean(false);

  private final boolean value;

  private KvBoolean(boolean value) {
    this.value = value;
  }

  public static KvBoolean from(boolean value) {
    return value ? TRUE : FALSE;
  }

  public boolean getPrimitiveValue() {
    return value;
  }

  @Override
  public Boolean getValue() {
    return value;
  }

  @Override
  public Class<? extends Boolean> getValueClass() {
    return Boolean.class;
  }

  @Override
  public BooleanType getType() {
    return BooleanType.INSTANCE;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }

  /**
   * The hashCode of a KvBoolean the same as {@link Boolean#hashCode() }
   * applied to its value.
   *
   * @return
   */
  @Override
  public int hashCode() {
    return Booleans.hashCode(getPrimitiveValue());
  }

  /**
   * Two KvBoolean are equal if their primitive values are equal.
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvBoolean)) {
      return false;
    }
    return this.getPrimitiveValue() == ((KvBoolean) obj).getPrimitiveValue();
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

}
