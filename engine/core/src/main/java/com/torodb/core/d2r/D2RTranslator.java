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

package com.torodb.core.d2r;

import com.torodb.kvdocument.values.KvDocument;

import javax.annotation.Nonnull;

/**
 *
 */
public interface D2RTranslator {

  /**
   * Translates from the document model to relational model the given argument.
   * <p>
   * The result is not returned on that function but appended to the accumulator associated with
   * this translator. In that way, several documents can be translated to the same
   * {@link CollectionData} to improve performance.
   * <p>
   * {@link #getCollectionDataAccumulator() } can be used to get the associated accumulator.
   *
   * @param doc the document that must be translated
   */
  public void translate(@Nonnull KvDocument doc);

  /**
   * A CollectionData that contains the translation of all documents that have been translated with
   * this translator.
   *
   * @return
   */
  @Nonnull
  public CollectionData getCollectionDataAccumulator();

}
