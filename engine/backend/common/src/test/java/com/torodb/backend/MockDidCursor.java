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

package com.torodb.backend;

import com.torodb.core.cursors.Cursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MockDidCursor implements Cursor<Integer> {

  private final Iterator<Integer> didsIterator;

  public MockDidCursor(Iterator<Integer> didsIterator) {
    super();
    this.didsIterator = didsIterator;
  }

  @Override
  public boolean hasNext() {
    return didsIterator.hasNext();
  }

  @Override
  public Integer next() {
    return didsIterator.next();
  }

  @Override
  public List<Integer> getNextBatch(final int maxSize) {
    List<Integer> dids = new ArrayList<>();

    for (int index = 0; index < maxSize && hasNext(); index++) {
      dids.add(next());
    }

    return dids;
  }

  @Override
  public List<Integer> getRemaining() {
    List<Integer> dids = new ArrayList<>();

    while (hasNext()) {
      dids.add(next());
    }

    return dids;
  }

  @Override
  public void close() {
  }
}
