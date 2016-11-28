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
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.document.ToroDocument;
import com.torodb.torod.cursors.TorodCursor;

import javax.annotation.Nonnull;

/**
 *
 */
public class LazyTorodCursor implements TorodCursor {

  private final R2DTranslator r2dTranslator;
  private final BackendCursor backendCursor;
  private FromBackendDocCursor docCursor;
  private boolean usedAsDidCursor = false;

  public LazyTorodCursor(@Nonnull R2DTranslator r2dTranslator,
      BackendCursor backendCursor) {
    this.r2dTranslator = r2dTranslator;
    this.backendCursor = backendCursor;
  }

  @Override
  public Cursor<ToroDocument> asDocCursor() {
    Preconditions.checkState(!usedAsDidCursor, "This cursor has already been used as a did cursor");

    if (docCursor == null) {
      docCursor = new FromBackendDocCursor(
          r2dTranslator, backendCursor.asDocPartResultCursor());
    }

    return docCursor;
  }

  @Override
  public Cursor<Integer> asDidCursor() {
    Preconditions.checkState(docCursor == null, "This cursor has already been used as a doc "
        + "cursor");
    usedAsDidCursor = true;
    return backendCursor.asDidCursor();
  }

}
