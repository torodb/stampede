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

package com.torodb.core.cursors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;

public interface Cursor<E> extends AutoCloseable, Iterator<E> {

  /**
   * Gets up to maxSize elements from the current position.
   *
   * @param maxSize
   * @return
   */
  @Nonnull
  default List<E> getNextBatch(int maxSize) {
    List<E> elements = new ArrayList<>(maxSize);

    for (int index = 0; index < maxSize && hasNext(); index++) {
      elements.add(next());
    }

    return elements;
  }

  /**
   * Gets remaining elements from the current position.
   *
   * @return
   */
  @Nonnull
  default List<E> getRemaining() {
    List<E> elements = new ArrayList<>();

    while (hasNext()) {
      elements.add(next());
    }
    return elements;
  }

  @Nonnull
  default <O> Cursor<O> transform(Function<E, O> transformation) {
    return new TransformCursor<>(this, transformation);
  }

  @Nonnull
  default BatchCursor<E> batch(int size) {
    return new BatchCursor<>(this, size);
  }

  /**
   * Releases this {@code Cursor} object's resources immediately.
   */
  @Override
  void close();

}
