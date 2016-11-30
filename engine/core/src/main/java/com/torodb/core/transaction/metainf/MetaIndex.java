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

import com.torodb.core.TableRef;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 */
public interface MetaIndex {

  /**
   * The name of the index on the doc model.
   *
   * @return
   */
  @Nonnull
  public abstract String getName();

  public abstract boolean isUnique();

  public abstract int size();

  public abstract Iterator<? extends MetaIndexField> iteratorFields();

  public abstract Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(
      TableRef tableRef);

  public abstract Stream<TableRef> streamTableRefs();

  @Nullable
  public abstract MetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name);

  @Nullable
  public abstract MetaIndexField getMetaIndexFieldByTableRefAndPosition(TableRef tableRef,
      int position);

  @Nullable
  public abstract MetaIndexField getMetaIndexFieldByPosition(int position);

  public abstract Iterator<List<String>> iteratorMetaDocPartIndexesIdentifiers(MetaDocPart docPart);

  public abstract boolean isCompatible(MetaDocPart docPart);

  public abstract boolean isCompatible(MetaDocPart docPart, MetaDocPartIndex docPartIndex);

  public abstract boolean isMatch(MetaDocPart docPart, List<String> identifiers,
      MetaDocPartIndex docPartIndex);

  public abstract boolean isMatch(MetaIndex index);

  public abstract boolean isSubMatch(MetaDocPart docPart, List<String> identifiersSublist,
      MetaDocPartIndex docPartIndex);

  public default String defautToString() {
    return "index{" + "name:" + getName() + ", unique:" + isUnique() + '}';
  }

}
