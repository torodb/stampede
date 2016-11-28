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

package com.torodb.torod.impl.sql;

import com.google.common.base.Preconditions;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

/**
 *
 */
public class FromBackendDocCursor implements Cursor<ToroDocument> {

  private static final int BATCH_SIZE = 1000;

  private final R2DTranslator r2dTranslator;
  private final Cursor<DocPartResult> docPartCursor;

  public FromBackendDocCursor(
      @Nonnull R2DTranslator r2dTranslator,
      @Nonnull Cursor<DocPartResult> docPartCursor) {
    this.r2dTranslator = r2dTranslator;
    this.docPartCursor = docPartCursor;
  }

  @Override
  public boolean hasNext() {
    return docPartCursor.hasNext();
  }

  @Override
  public ToroDocument next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return getNextBatch(1).get(0);
  }

  @Override
  public List<ToroDocument> getRemaining() {
    List<ToroDocument> allDocuments = new ArrayList<>();

    List<ToroDocument> readedDocuments;
    while (docPartCursor.hasNext()) {
      readedDocuments = getNextBatch(BATCH_SIZE);
      allDocuments.addAll(readedDocuments);
    }

    return allDocuments;
  }

  @Override
  public List<ToroDocument> getNextBatch(int maxResults) {
    Preconditions.checkArgument(maxResults > 0, "max results must be at "
        + "least 1, but " + maxResults + " was recived");

    if (!docPartCursor.hasNext()) {
      return Collections.emptyList();
    }

    List<DocPartResult> nextDpBatch = docPartCursor.getNextBatch(maxResults);

    List<ToroDocument> translated = r2dTranslator.translate(
        nextDpBatch.iterator());

    nextDpBatch.forEach(docPartResult -> docPartResult.close());

    return translated;
  }

  @Override
  public void forEachRemaining(Consumer<? super ToroDocument> action) {
    getRemaining().forEach(action);
  }

  @Override
  public void close() {
    docPartCursor.close();
  }
}
