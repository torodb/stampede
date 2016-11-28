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

package com.torodb.core.language;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 *
 */
@Immutable
public class AttributeReference implements Serializable {

  public static final AttributeReference EMPTY_REFERENCE = new AttributeReference(Collections
      .<Key<?>>emptyList());
  private static final long serialVersionUID = 1L;

  @Nonnull
  private final List<Key<?>> keys;

  public AttributeReference(@Nonnull List<Key<?>> keys) {
    this.keys = ImmutableList.copyOf(keys);
  }

  @Nonnull
  public List<Key<?>> getKeys() {
    return keys;
  }

  /**
   *
   * @param from
   * @param to   not included
   * @return
   */
  public AttributeReference subReference(int from, int to) {
    return new AttributeReference(keys.subList(from, to));
  }

  public AttributeReference append(AttributeReference.Key<?> tail) {
    return new AttributeReference(
        ImmutableList.<AttributeReference.Key<?>>builder()
            .addAll(keys)
            .add(tail)
            .build()
    );
  }

  public AttributeReference append(AttributeReference tail) {
    return append(tail.getKeys());
  }

  public AttributeReference append(List<? extends Key<?>> tail) {
    ImmutableList<Key<?>> newList = ImmutableList.copyOf(Iterables.concat(this.keys, tail));
    return new AttributeReference(newList);
  }

  public AttributeReference prepend(List<? extends Key<?>> head) {
    return new AttributeReference(ImmutableList.copyOf(Iterables.concat(head, this.keys)));
  }

  @Override
  public String toString() {
    return Joiner.on(".").join(keys);
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 67 * hash + this.keys.hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final AttributeReference other = (AttributeReference) obj;
    if (this.keys != other.keys && !this.keys.equals(other.keys)) {
      return false;
    }
    return true;
  }

  public static class Builder {

    private final List<Key<?>> keys;

    public Builder() {
      keys = Lists.newArrayList();
    }

    public Builder addObjectKeyAsFirst(String key) {
      keys.add(0, new ObjectKey(key));
      return this;
    }

    public Builder addArrayKeyAsFirst(int key) {
      keys.add(0, new ArrayKey(key));
      return this;
    }

    public Builder addObjectKey(String key) {
      keys.add(new ObjectKey(key));
      return this;
    }

    public Builder addArrayKey(int key) {
      keys.add(new ArrayKey(key));
      return this;
    }

    public AttributeReference build() {
      return new AttributeReference(ImmutableList.copyOf(keys));
    }
  }

  public static interface Key<K> extends Serializable {

    public String getTypeAsString();

    public K getKeyValue();
  }

  public static class ObjectKey implements Key<String>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String key;

    public ObjectKey(String key) {
      this.key = key;
    }

    @Override
    public String getKeyValue() {
      return key;
    }

    public String getKey() {
      return key;
    }

    @Override
    public String toString() {
      return key;
    }

    @Override
    public String getTypeAsString() {
      return "object key";
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 23 * hash + (this.key != null ? this.key.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ObjectKey other = (ObjectKey) obj;
      if ((this.key == null) ? (other.key != null) : !this.key.equals(other.key)) {
        return false;
      }
      return true;
    }
  }

  public static class ArrayKey implements Key<Integer>, Serializable {

    private static final long serialVersionUID = 1L;

    private final int index;

    public ArrayKey(int index) {
      this.index = index;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public Integer getKeyValue() {
      return index;
    }

    @Override
    public String toString() {
      return Integer.toString(index);
    }

    @Override
    public String getTypeAsString() {
      return "array key";
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 61 * hash + this.index;
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ArrayKey other = (ArrayKey) obj;
      if (this.index != other.index) {
        return false;
      }
      return true;
    }
  }
}
