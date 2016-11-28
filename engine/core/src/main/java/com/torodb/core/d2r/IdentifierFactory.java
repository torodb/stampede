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

import com.torodb.core.TableRef;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaSnapshot;
import org.jooq.lambda.tuple.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface IdentifierFactory {

  @Nonnull
  String toDatabaseIdentifier(@Nonnull MetaSnapshot metaSnapshot, @Nonnull String database);

  @Nonnull
  String toCollectionIdentifier(@Nonnull MetaSnapshot metaSnapshot, @Nonnull String database,
      @Nonnull String collection);

  @Nonnull
  String toDocPartIdentifier(@Nonnull MetaDatabase metaDatabase, @Nonnull String collection,
      @Nonnull TableRef tableRef);

  @Nonnull
  String toFieldIdentifier(@Nonnull MetaDocPart metaDocPart, @Nonnull String field,
      @Nonnull FieldType fieldType);

  @Nonnull
  String toFieldIdentifierForScalar(@Nonnull FieldType fieldType);

  @Nonnull
  String toIndexIdentifier(@Nonnull MetaDatabase metaSnapshot, String tableName,
      @Nonnull Iterable<Tuple2<String, Boolean>> identifiers);
}
