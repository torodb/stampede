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

import java.util.Optional;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaCollection extends MetaCollection {

  /**
   * Returns the {@link ImmutableMetaCollection} from which this one derivates.
   */
  public ImmutableMetaCollection getOrigin();

  @Override
  public MutableMetaDocPart getMetaDocPartByTableRef(TableRef tableRef);

  @Override
  public MutableMetaDocPart getMetaDocPartByIdentifier(String docPartId);

  @Override
  public MutableMetaIndex getMetaIndexByName(String indexId);

  @Override
  public Stream<? extends MutableMetaDocPart> streamContainedMetaDocParts();

  public MutableMetaDocPart addMetaDocPart(TableRef tableRef, String identifier) throws
      IllegalArgumentException;

  public Stream<? extends MutableMetaDocPart> streamModifiedMetaDocParts();

  @Override
  public Stream<? extends MutableMetaIndex> streamContainedMetaIndexes();

  public MutableMetaIndex addMetaIndex(String name, boolean unique) throws IllegalArgumentException;

  public boolean removeMetaIndexByName(String indexName);

  public Stream<ChangedElement<MutableMetaIndex>> streamModifiedMetaIndexes();

  public default Optional<ChangedElement<MutableMetaIndex>> getModifiedMetaIndexByName(
      String idxName) {
    return streamModifiedMetaIndexes()
        .filter(change -> change.getElement().getName().equals(idxName))
        .findAny();
  }

  public Optional<ImmutableMetaDocPart> getAnyDocPartWithMissedDocPartIndex(
      ImmutableMetaCollection oldStructure, MutableMetaIndex changed);

  public Optional<? extends MetaIdentifiedDocPartIndex> getAnyOrphanDocPartIndex(
      ImmutableMetaCollection oldStructure, MutableMetaIndex changed);
}
