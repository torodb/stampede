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

import com.google.common.base.Preconditions;

import java.util.Iterator;

/**
 *
 */
public class IteratorCursor<E> implements Cursor<E> {

  private boolean closed = false;
  private final Iterator<E> iterator;

  public IteratorCursor(Iterator<E> iterator) {
    this.iterator = iterator;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      iterator.forEachRemaining(e -> {
      });
    }
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(!closed, "The cursor is closed");
    return iterator.hasNext();
  }

  @Override
  public E next() {
    Preconditions.checkState(!closed, "The cursor is closed");
    return iterator.next();
  }

}
