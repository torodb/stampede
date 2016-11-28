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

import com.torodb.core.annotations.DoNotChange;
import org.jooq.lambda.tuple.Tuple2;

import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaDatabase extends MetaDatabase {

  @Override
  public MutableMetaCollection getMetaCollectionByIdentifier(String collectionIdentifier);

  @Override
  public MutableMetaCollection getMetaCollectionByName(String collectionName);

  @Override
  public Stream<? extends MutableMetaCollection> streamMetaCollections();

  public abstract MutableMetaCollection addMetaCollection(String colName, String colId) throws
      IllegalArgumentException;

  /**
   * Removes a meta collection selected by its name.
   *
   * @param collectionName
   * @return true iff the meta collection was removed
   */
  public abstract boolean removeMetaCollectionByName(String collectionName);

  /**
   * REmoves a meta collection selected by its identifier
   *
   * @param collectionId
   * @return true iff the meta collection was removed
   */
  public abstract boolean removeMetaCollectionByIdentifier(String collectionId);

  @DoNotChange
  @SuppressWarnings("checkstyle:LineLength")
  public abstract Iterable<Tuple2<MutableMetaCollection, MetaElementState>> getModifiedCollections();

  public abstract ImmutableMetaDatabase immutableCopy();

}
