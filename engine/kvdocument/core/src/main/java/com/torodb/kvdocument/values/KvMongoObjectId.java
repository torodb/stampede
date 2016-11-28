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

import com.torodb.common.util.HexUtils;
import com.torodb.kvdocument.types.MongoObjectIdType;

import java.util.Arrays;

/**
 *
 */
public abstract class KvMongoObjectId extends KvValue<KvMongoObjectId> {

  private static final long serialVersionUID = -2548014610816031841L;

  /**
   * Returns an array that contains the same bytes that {@linkplain
   * #getValue()}.
   * <p>
   * Modifications in the given array won't affect the stored value
   * <p>
   * @return
   */
  public abstract byte[] getArrayValue();

  protected byte[] getBytesUnsafe() {
    return getArrayValue();
  }

  @Override
  public KvMongoObjectId getValue() {
    return this;
  }

  @Override
  public Class<? extends KvMongoObjectId> getValueClass() {
    return getClass();
  }

  @Override
  public String toString() {
    return HexUtils.bytes2Hex(getArrayValue());
  }

  @Override
  public MongoObjectIdType getType() {
    return MongoObjectIdType.INSTANCE;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(getBytesUnsafe());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof KvMongoObjectId)) {
      return false;
    }
    return Arrays.equals(this.getArrayValue(), ((KvMongoObjectId) obj).getArrayValue());
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

}
