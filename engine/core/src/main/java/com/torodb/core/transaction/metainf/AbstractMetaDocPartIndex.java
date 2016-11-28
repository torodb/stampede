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

package com.torodb.core.transaction.metainf;

import java.util.Iterator;

/**
 *
 */
public abstract class AbstractMetaDocPartIndex implements MetaDocPartIndex {

  private final boolean unique;

  public AbstractMetaDocPartIndex(boolean unique) {
    this.unique = unique;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }

  @Override
  public boolean hasSameColumns(MetaDocPartIndex docPartIndex) {
    return hasSameColumns(docPartIndex, iteratorColumns());
  }

  protected boolean hasSameColumns(MetaDocPartIndex docPartIndex,
      Iterator<? extends MetaDocPartIndexColumn> columnsIterator) {
    Iterator<? extends MetaDocPartIndexColumn> docPartIndexColumnsIterator = docPartIndex
        .iteratorColumns();

    while (columnsIterator.hasNext() && docPartIndexColumnsIterator.hasNext()) {
      MetaDocPartIndexColumn column = columnsIterator.next();
      MetaDocPartIndexColumn docPartIndexColumn = docPartIndexColumnsIterator.next();
      if (!column.getIdentifier().equals(docPartIndexColumn.getIdentifier()) || column.getOrdering()
          != docPartIndexColumn.getOrdering()) {
        return false;
      }
    }

    return !columnsIterator.hasNext() && !docPartIndexColumnsIterator.hasNext();
  }

  @Override
  public String toString() {
    return defautToString();
  }

}
