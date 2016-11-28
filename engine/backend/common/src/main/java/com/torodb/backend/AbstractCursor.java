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

import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.cursors.Cursor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

public abstract class AbstractCursor<T> implements Cursor<T> {

  public final ErrorHandler errorHandler;
  public final ResultSet resultSet;
  public boolean movedNext = false;
  public boolean hasNext = false;

  public AbstractCursor(@Nonnull ErrorHandler errorHandler, @Nonnull ResultSet resultSet) {
    this.errorHandler = errorHandler;
    this.resultSet = resultSet;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!movedNext) {
        hasNext = resultSet.next();
        movedNext = true;
      }

      return hasNext;
    } catch (SQLException ex) {
      throw errorHandler.handleException(Context.FETCH, ex);
    }
  }

  @Override
  public T next() {
    try {
      hasNext();
      movedNext = false;

      return read(resultSet);
    } catch (SQLException ex) {
      throw errorHandler.handleException(Context.FETCH, ex);
    }
  }

  protected abstract T read(ResultSet resultSet) throws SQLException;

  @Override
  public void close() {
    try {
      resultSet.close();
    } catch (SQLException ex) {
      throw errorHandler.handleException(Context.FETCH, ex);
    }
  }

  @Override
  public List<T> getNextBatch(final int maxSize) {
    List<T> batch = new ArrayList<>();

    for (int index = 0; index < maxSize && hasNext(); index++) {
      batch.add(next());
    }

    return batch;
  }

  @Override
  public List<T> getRemaining() {
    List<T> batch = new ArrayList<>();

    while (hasNext()) {
      batch.add(next());
    }

    return batch;
  }
}
