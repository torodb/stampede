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

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.torodb.kvdocument.annotations.NotMutable;
import com.torodb.kvdocument.values.KvArray;
import com.torodb.kvdocument.values.KvValue;

import java.util.List;

import javax.annotation.Nonnull;

/**
 *
 */
public class ListKvArray extends KvArray {

  private static final long serialVersionUID = -5242307037136472681L;

  private final List<KvValue<?>> list;

  public ListKvArray(@NotMutable List<KvValue<?>> list) {
    this.list = list;
  }

  @Override
  public UnmodifiableIterator<KvValue<?>> iterator() {
    return Iterators.unmodifiableIterator(list.iterator());
  }

  @Override
  protected boolean equalsOptimization(@Nonnull KvArray other) {
    if (other instanceof ListKvArray) {
      return this.size() == other.size();
    }
    return true;
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public boolean contains(KvValue<?> element) {
    return list.contains(element);
  }

  @Override
  public KvValue<?> get(int index) throws IndexOutOfBoundsException {
    return list.get(index);
  }
}
