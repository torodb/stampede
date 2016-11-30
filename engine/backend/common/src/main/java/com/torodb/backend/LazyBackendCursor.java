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
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import org.jooq.DSLContext;

import javax.annotation.Nonnull;

/**
 *
 */
public class LazyBackendCursor implements BackendCursor {

  private final Cursor<Integer> didCursor;
  private final DefaultDocPartResultCursor docCursor;
  private boolean usedAsDocPartCursor = false;
  private boolean usedAsDidCursor = false;

  public LazyBackendCursor(
      @Nonnull SqlInterface sqlInterface,
      final @Nonnull Cursor<Integer> didCursor,
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase metaDatabase,
      @Nonnull MetaCollection metaCollection) {
    docCursor = new DefaultDocPartResultCursor(sqlInterface, didCursor, dsl, metaDatabase,
        metaCollection);
    this.didCursor = didCursor;
  }

  @Override
  public Cursor<DocPartResult> asDocPartResultCursor() {
    Preconditions.checkState(!usedAsDidCursor, "This cursor has already been used as a did cursor");
    usedAsDocPartCursor = true;
    return docCursor;
  }

  @Override
  public Cursor<Integer> asDidCursor() {
    Preconditions.checkState(!usedAsDocPartCursor,
        "This cursor has already been used as a doc part cursor");
    usedAsDidCursor = true;
    return didCursor;
  }
}
