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

package com.torodb.core.backend;

import com.google.common.collect.Multimap;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KvValue;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Optional;

/**
 *
 */
public interface BackendTransaction extends AutoCloseable {

  public long getDatabaseSize(MetaDatabase db);

  public long countAll(MetaDatabase db, MetaCollection col);

  public long getCollectionSize(MetaDatabase db, MetaCollection col);

  public long getDocumentsSize(MetaDatabase db, MetaCollection col);

  public BackendCursor fetch(MetaDatabase db, MetaCollection col, Cursor<Integer> didCursor);

  public BackendCursor findAll(MetaDatabase db, MetaCollection col);

  public BackendCursor findByField(MetaDatabase db, MetaCollection col,
      MetaDocPart docPart, MetaField field, KvValue<?> value);

  /**
   * Return a cursor that iterates over all documents that fulfill the query.
   *
   * Each entry on the metafield is a value restriction on the entry metafield. The query is
   * fulfilled if for at least one entry, the evaluation is true.
   *
   * @param db
   * @param col
   * @param docPart
   * @param valuesMultimap
   * @return
   */
  public BackendCursor findByFieldIn(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap);

  /**
   * Return a cursor that iterates over all dids associated with the relative value that fulfill the
   * query.
   *
   * Each entry on the metafield is a value restriction on the entry metafield. The query is
   * fulfilled if for at least one entry, the evaluation is true.
   *
   * @param db
   * @param col
   * @param docPart
   * @param valuesMultimap
   * @return
   */
  public Cursor<Tuple2<Integer, KvValue<?>>> findByFieldInProjection(MetaDatabase db,
      MetaCollection col, MetaDocPart docPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap);

  /**
   * Reads the metadata value stored with the given key.
   *
   * This metainfo is a key-value storage that different modules can use to store their own
   * information.
   *
   * @param key
   * @return
   */
  public Optional<KvValue<?>> readMetaInfo(MetaInfoKey key);

  public void checkMetaDataTables() throws InvalidDatabaseException;

  public void rollback();

  @Override
  public void close();
}
