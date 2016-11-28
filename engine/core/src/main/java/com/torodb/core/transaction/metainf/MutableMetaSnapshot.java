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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 * @param <MMD>
 */
public interface MutableMetaSnapshot extends MetaSnapshot {

  @Override
  @Nullable
  public MutableMetaDatabase getMetaDatabaseByIdentifier(String dbIdentifier);

  @Override
  @Nullable
  public MutableMetaDatabase getMetaDatabaseByName(String dbName);

  @Override
  public Stream<? extends MutableMetaDatabase> streamMetaDatabases();

  @Nonnull
  public abstract MutableMetaDatabase addMetaDatabase(String dbName, String dbId) throws
      IllegalArgumentException;

  /**
   * Removes a meta database selected by its name.
   *
   * @param dbName
   * @return true iff the meta database was removed
   */
  public abstract boolean removeMetaDatabaseByName(String dbName);

  /**
   * REmoves a meta database selected by its identifier
   *
   * @param dbId
   * @return true iff the meta database was removed
   */
  public abstract boolean removeMetaDatabaseByIdentifier(String dbId);

  @DoNotChange
  public abstract Iterable<Tuple2<MutableMetaDatabase, MetaElementState>> getModifiedDatabases();

  public abstract boolean hasChanged();

  @Nonnull
  public abstract ImmutableMetaSnapshot immutableCopy();

  public default boolean containsMetaDatabaseByName(String dbName) {
    return getMetaDatabaseByName(dbName) != null;
  }

  public default boolean containsMetaDatabaseByIdentifier(String dbId) {
    return getMetaDatabaseByIdentifier(dbId) != null;
  }
}
