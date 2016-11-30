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

import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public interface MutableMetaDocPart extends MetaDocPart {

  @Override
  public ImmutableMetaField getMetaFieldByNameAndType(String fieldName, FieldType type);

  @Override
  public Stream<? extends ImmutableMetaField> streamMetaFieldByName(String fieldName);

  @Override
  public ImmutableMetaField getMetaFieldByIdentifier(String fieldId);

  @Override
  public Stream<? extends ImmutableMetaField> streamFields();

  @Override
  public Stream<? extends MetaScalar> streamScalars();

  @Override
  public abstract Stream<? extends MetaIdentifiedDocPartIndex> streamIndexes();

  /**
   * Adds a new field to this table.
   *
   * @param name
   * @param identifier
   * @param type
   * @return the new column
   * @throws IllegalArgumentException if this table already contains a column with the same
   *                                  {@link DbColumn#getIdentifier() id} or with the same pair
   *                                  {@link DbColumn#getName() name} and
   *                                  {@link DbColumn#getType() type}.
   */
  public abstract ImmutableMetaField addMetaField(String name, String identifier, FieldType type)
      throws IllegalArgumentException;

  /**
   *
   * @return @throws IllegalArgumentException if this table already contains a scalar with the same
   *         type or name
   */
  public abstract ImmutableMetaScalar addMetaScalar(String identifier, FieldType type) throws
      IllegalArgumentException;

  @DoNotChange
  public abstract Iterable<? extends ImmutableMetaField> getAddedMetaFields();

  public abstract ImmutableMetaField getAddedFieldByIdentifier(String identifier);

  @DoNotChange
  public abstract Iterable<? extends ImmutableMetaScalar> getAddedMetaScalars();

  /**
   * Add a non existent index to this doc part
   *
   * @param unique
   * @return
   */
  public abstract MutableMetaDocPartIndex addMetaDocPartIndex(boolean unique);

  /**
   * Remove an index from this doc part
   *
   * @param indexId
   * @return
   */
  public boolean removeMetaDocPartIndexByIdentifier(String indexId);

  @DoNotChange
  @SuppressWarnings("checkstyle:LineLength")
  public Iterable<Tuple2<ImmutableMetaIdentifiedDocPartIndex, MetaElementState>> getModifiedMetaDocPartIndexes();

  @DoNotChange
  public Iterable<MutableMetaDocPartIndex> getAddedMutableMetaDocPartIndexes();

  public MutableMetaDocPartIndex getOrCreatePartialMutableDocPartIndexForMissingIndexAndNewField(
      MetaIndex missingIndex, List<String> identifiers, MetaField newField);

  public abstract ImmutableMetaDocPart immutableCopy();
}
