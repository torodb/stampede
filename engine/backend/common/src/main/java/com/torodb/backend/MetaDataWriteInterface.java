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

package com.torodb.backend;

import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaDocPartIndexColumn;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MetaIndexField;
import com.torodb.core.transaction.metainf.MetaScalar;
import org.jooq.DSLContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface MetaDataWriteInterface {

  void createMetaDatabaseTable(@Nonnull DSLContext dsl);

  void createMetaCollectionTable(@Nonnull DSLContext dsl);

  void createMetaDocPartTable(@Nonnull DSLContext dsl);

  void createMetaFieldTable(@Nonnull DSLContext dsl);

  void createMetaScalarTable(@Nonnull DSLContext dsl);

  void createMetaIndexTable(@Nonnull DSLContext dsl);

  void createMetaIndexFieldTable(@Nonnull DSLContext dsl);

  void createMetaDocPartIndexTable(@Nonnull DSLContext dsl);

  void createMetaFieldIndexTable(@Nonnull DSLContext dsl);

  void createKvTable(@Nonnull DSLContext dsl);

  void addMetaDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database);

  void addMetaCollection(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection);

  void addMetaDocPart(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart);

  void addMetaField(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaField field);

  void addMetaScalar(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, @Nonnull MetaScalar scalar);

  void addMetaIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaIndex index);

  void addMetaIndexField(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaIndex index, @Nonnull MetaIndexField field);

  void addMetaDocPartIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart,
      @Nonnull MetaIdentifiedDocPartIndex index);

  void addMetaDocPartIndexColumn(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart,
      @Nonnull MetaIdentifiedDocPartIndex index, @Nonnull MetaDocPartIndexColumn field);

  void deleteMetaDatabase(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database);

  void deleteMetaCollection(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection);

  void deleteMetaIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaIndex index);

  void deleteMetaDocPartIndex(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart,
      @Nonnull MetaIdentifiedDocPartIndex index);

  int consumeRids(@Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull MetaDocPart docPart, int count);

  @Nullable
  String writeMetaInfo(@Nonnull DSLContext dsl, @Nonnull MetaInfoKey key, @Nonnull String newValue);
}
