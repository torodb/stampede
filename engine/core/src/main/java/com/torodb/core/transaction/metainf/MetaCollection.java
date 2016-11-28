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
import org.jooq.lambda.tuple.Tuple2;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 */
public interface MetaCollection {

  /**
   * The name of the collection on the doc model.
   *
   * @return
   */
  @Nonnull
  public abstract String getName();

  /**
   * The identifier of the collection on the SQL model.
   *
   * @return
   */
  @Nonnull
  public abstract String getIdentifier();

  public abstract Stream<? extends MetaDocPart> streamContainedMetaDocParts();

  @Nullable
  public abstract MetaDocPart getMetaDocPartByIdentifier(String docPartId);

  @Nullable
  public abstract MetaDocPart getMetaDocPartByTableRef(TableRef tableRef);

  public Stream<? extends MetaIndex> streamContainedMetaIndexes();

  @Nullable
  public MetaIndex getMetaIndexByName(String indexName);

  public List<Tuple2<MetaIndex, List<String>>> getMissingIndexesForNewField(
      MutableMetaDocPart docPart, MetaField newField);

  public default String defautToString() {
    return "col{" + "name:" + getName() + ", id:" + getIdentifier() + '}';
  }

}
