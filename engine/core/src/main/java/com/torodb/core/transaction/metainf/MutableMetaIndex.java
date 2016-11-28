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
import com.torodb.core.annotations.DoNotChange;

import java.util.Iterator;

/**
 *
 */
public interface MutableMetaIndex extends MetaIndex {

  public abstract boolean isUnique();

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByPosition(int position);

  @Override
  public ImmutableMetaIndexField getMetaIndexFieldByTableRefAndName(TableRef tableRef, String name);

  @Override
  public Iterator<? extends ImmutableMetaIndexField> iteratorFields();

  @Override
  public Iterator<? extends ImmutableMetaIndexField> iteratorMetaIndexFieldByTableRef(
      TableRef tableRef);

  /**
   * Adds a new field to this index.
   *
   * @param name
   * @return the new field
   * @throws IllegalArgumentException if this index already contains a field with the same
   *                                  {@link MetaIndexField#getPosition() position} or with the same
   *                                  pair {@link MetaIndexField#getTableRef() tableRef} and
   *                                  {@link MetaIndexField#getName() name}.
   */
  public abstract ImmutableMetaIndexField addMetaIndexField(TableRef tableRef, String name,
      FieldIndexOrdering ordering) throws IllegalArgumentException;

  @DoNotChange
  public abstract Iterable<? extends ImmutableMetaIndexField> getAddedMetaIndexFields();

  public abstract ImmutableMetaIndex immutableCopy();
}
