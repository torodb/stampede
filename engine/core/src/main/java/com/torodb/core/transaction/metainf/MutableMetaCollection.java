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
import org.jooq.lambda.tuple.Tuple2;

import java.util.Optional;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaCollection extends MetaCollection {

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

  @DoNotChange
  public Iterable<? extends MutableMetaDocPart> getModifiedMetaDocParts();

  @Override
  public Stream<? extends MutableMetaIndex> streamContainedMetaIndexes();

  public MutableMetaIndex addMetaIndex(String name, boolean unique) throws IllegalArgumentException;

  public boolean removeMetaIndexByName(String indexName);

  @DoNotChange
  public Iterable<Tuple2<MutableMetaIndex, MetaElementState>> getModifiedMetaIndexes();

  public Optional<? extends MetaIndex> getAnyMissedIndex(MetaCollection oldCol,
      MutableMetaDocPart newStructure, ImmutableMetaDocPart oldStructure,
      ImmutableMetaField newField);

  public Optional<ImmutableMetaIndex> getAnyMissedIndex(ImmutableMetaCollection oldCol,
      ImmutableMetaIdentifiedDocPartIndex newRemovedDocPartIndex);

  public Optional<? extends MetaIndex> getAnyRelatedIndex(ImmutableMetaCollection oldCol,
      MetaDocPart newStructure, ImmutableMetaIdentifiedDocPartIndex newDocPartIndex);

  public Optional<ImmutableMetaIndex> getAnyConflictingIndex(
      ImmutableMetaCollection oldStructure, MutableMetaIndex changed);

  public Optional<ImmutableMetaDocPart> getAnyDocPartWithMissedDocPartIndex(
      ImmutableMetaCollection oldStructure, MutableMetaIndex changed);

  public Optional<? extends MetaIdentifiedDocPartIndex> getAnyOrphanDocPartIndex(
      ImmutableMetaCollection oldStructure, MutableMetaIndex changed);

  public abstract ImmutableMetaCollection immutableCopy();
}
