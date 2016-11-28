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

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 */
public interface MetaDocPart {

  @Nonnull
  public abstract TableRef getTableRef();

  /**
   * The identifier MetaDocPart on the SQL model.
   *
   * @return
   */
  @Nonnull
  public abstract String getIdentifier();

  @Nonnull
  @DoNotChange
  public abstract Stream<? extends MetaField> streamFields();

  /**
   *
   * @param fieldId
   * @return the contained column whose {@link MetaField#getDbName() db name} is the given or null
   *         if there is no one that match that condition
   */
  @Nullable
  public abstract MetaField getMetaFieldByIdentifier(String fieldId);

  /**
   *
   * @param fieldName
   * @return the contained columns whose {@link MetaField#getDocName() db name} is the given or an
   *         empty list if there is no one that match that condition
   */
  public abstract Stream<? extends MetaField> streamMetaFieldByName(String fieldName);

  /**
   *
   * @param fieldName
   * @param type
   * @return the contained column whose {@link MetaField#getDocName() db name} and whose
   *         {@link MetaField#getType() type} is the given or null if there is no one that match
   *         that condition
   */
  @Nullable
  public abstract MetaField getMetaFieldByNameAndType(String fieldName, FieldType type);

  public abstract Stream<? extends MetaScalar> streamScalars();

  @Nullable
  public default MetaScalar getScalar(@Nonnull FieldType type) {
    return streamScalars().filter((s) -> s.getType() == type).findAny().orElse(null);
  }

  @Nullable
  public default MetaScalar getScalar(@Nonnull String identifier) {
    return streamScalars().filter((s) -> s.getIdentifier().equals(identifier)).findAny().orElse(
        null);
  }

  @Nonnull
  @DoNotChange
  public abstract Stream<? extends MetaIdentifiedDocPartIndex> streamIndexes();

  /**
   *
   * @param indexId
   * @return the contained index whose {@link MetaIdentifiedDocPartIndex#getidentifier() indexId} is
   *         the given or null if there is no one that match that condition
   */
  @Nullable
  public abstract MetaIdentifiedDocPartIndex getMetaDocPartIndexByIdentifier(String indexId);

  public default String defautToString() {
    return "docPart{" + "ref:" + getTableRef() + ", id:" + getIdentifier() + '}';
  }
}
