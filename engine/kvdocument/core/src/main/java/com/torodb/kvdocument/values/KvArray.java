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

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.hash.Hashing;
import com.torodb.kvdocument.types.ArrayType;
import com.torodb.kvdocument.types.GenericType;
import com.torodb.kvdocument.types.KvType;

import javax.annotation.Nonnull;

public abstract class KvArray extends KvValue<KvArray> implements Iterable<KvValue<?>> {

  private static final long serialVersionUID = -1293533707257230132L;

  private ArrayType type = null;

  @Override
  public abstract UnmodifiableIterator<KvValue<?>> iterator();

  @Override
  public KvArray getValue() {
    return this;
  }

  @Override
  public Class<? extends KvArray> getValueClass() {
    return this.getClass();
  }

  @Nonnull
  public KvType getElementType() {
    return getType().getElementType();
  }

  @Override
  public ArrayType getType() {
    if (type == null) {
      type = new ArrayType(calculateElementType(this));
    }
    return type;
  }

  @Override
  public <R, A> R accept(KvValueVisitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  @Nonnull
  public KvValue<?> get(int index) throws IndexOutOfBoundsException {
    return Iterables.get(this, index);
  }

  public boolean contains(KvValue<?> element) {
    return Iterables.contains(this, element);
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public int size() {
    return Iterables.size(this);
  }

  @Override
  public String toString() {
    return Iterables.toString(this);
  }

  @Override
  public int hashCode() {
    return Hashing.goodFastHash(32).newHasher().putInt(size()).putInt(getElementType().hashCode())
        .hash().asInt();
  }

  /**
   * Implementations can override this method to optimize the equality check.
   *
   * Some implementations can optimize the equality check using specific improvements. For example
   * an implementation that uses an ArrayList can check if another KvArray is different by checking
   * their sizes, which is O(1) on ArrayLists instead of O(n) in general implementations.
   *
   * <p/>If this method return true,
   * {@link Iterables#elementsEqual(java.lang.Iterable, java.lang.Iterable)} will be called to
   * check if the content of both KvArray are equal. If it return false,
   * {@link #equals(java.lang.Object)} will return false without iterating over the KVArrays.
   *
   * @param other
   * @return
   */
  protected boolean equalsOptimization(@Nonnull KvArray other) {
    return true;
  }

  /**
   * Two ArrayValues values are equal if their contains equal elements in the same position.
   *
   * An easy way to implement that is to delegate on
     * {@link Iterators#elementsEqual(java.lang.Iterator, java.lang.Iterator) }
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
    if (!(obj instanceof KvArray)) {
      return false;
    }
    KvArray other = (KvArray) obj;
    if (!other.getElementType().equals(this.getElementType())) {
      return false;
    }
    if (!equalsOptimization(other)) {
      return false;
    }
    return Iterables.elementsEqual(other, this);
  }

  @Nonnull
  protected static KvType calculateElementType(Iterable<KvValue<?>> iterable) {
    KvType result = null;
    for (KvValue<?> kVValue : iterable) {
      KvType iestType = kVValue.getType();
      if (result == null) {
        result = iestType;
      } else if (!result.equals(iestType)) {
        result = GenericType.INSTANCE;
        break;
      }
    }
    if (result == null) {
      result = GenericType.INSTANCE;
    }

    return result;
  }
}
