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

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.jooq.DSLContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 *
 */
public class DefaultDocPartResultCursor implements Cursor<DocPartResult> {

  private static final int BATCH_SIZE = 1000;

  private final SqlInterface sqlInterface;
  private final Cursor<Integer> didCursor;
  private final DSLContext dsl;
  private final MetaDatabase metaDatabase;
  private final MetaCollection metaCollection;

  public DefaultDocPartResultCursor(
      @Nonnull SqlInterface sqlInterface,
      @Nonnull Cursor<Integer> didCursor,
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase metaDatabase,
      @Nonnull MetaCollection metaCollection) {
    this.sqlInterface = sqlInterface;
    this.didCursor = didCursor;
    this.dsl = dsl;
    this.metaDatabase = metaDatabase;
    this.metaCollection = metaCollection;
  }

  @Override
  public boolean hasNext() {
    return didCursor.hasNext();
  }

  @Override
  public DocPartResult next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return getNextBatch(1).get(0);
  }

  @Override
  public List<DocPartResult> getRemaining() {
    List<DocPartResult> allDocuments = new ArrayList<>();

    List<DocPartResult> readedDocuments;
    while (didCursor.hasNext()) {
      readedDocuments = getNextBatch(BATCH_SIZE);
      allDocuments.addAll(readedDocuments);
    }

    return allDocuments;
  }

  @Override
  public List<DocPartResult> getNextBatch(int maxResults) {
    Preconditions.checkArgument(maxResults > 0, "max results must be at least 1, but " + maxResults
        + " was recived");

    if (!didCursor.hasNext()) {
      return Collections.emptyList();
    }

    try {
      return sqlInterface.getReadInterface().getCollectionResultSets(
          dsl, metaDatabase, metaCollection, didCursor, maxResults
      );
    } catch (SQLException ex) {
      throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
    }
  }

  @Override
  public void forEachRemaining(Consumer<? super DocPartResult> action) {
    getRemaining().forEach(action);
  }

  @Override
  public void close() {
    didCursor.close();
  }
}
