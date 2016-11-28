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

import javax.annotation.Nullable;

/**
 */
public interface MetaDocPartIndex {

  public abstract boolean isUnique();

  public abstract int size();

  public abstract Iterator<? extends MetaDocPartIndexColumn> iteratorColumns();

  @Nullable
  public abstract MetaDocPartIndexColumn getMetaDocPartIndexColumnByPosition(int position);

  @Nullable
  public abstract MetaDocPartIndexColumn getMetaDocPartIndexColumnByIdentifier(String columnName);

  public abstract boolean hasSameColumns(MetaDocPartIndex docPartIndex);

  public default String defautToString() {
    return "docPartIndex{" + "unique:" + isUnique() + '}';
  }

}
