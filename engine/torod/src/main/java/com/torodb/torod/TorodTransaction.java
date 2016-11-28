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

package com.torodb.torod;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.cursors.TorodCursor;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public interface TorodTransaction extends AutoCloseable {

  public boolean isClosed();

  public TorodConnection getConnection();

  public boolean existsDatabase(String dbName);

  public boolean existsCollection(String dbName, String colName);

  public List<String> getDatabases();

  public long getDatabaseSize(String dbName);

  public long countAll(String dbName, String colName);

  public long getCollectionSize(String dbName, String colName);

  public long getDocumentsSize(String dbName, String colName);

  public TorodCursor findAll(String dbName, String colName);

  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value);

  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values);

  /**
   * Like {@link #findByAttRefIn(java.lang.String, java.lang.String,
   * com.torodb.core.language.AttributeReference, java.util.Collection)
   * }, but the returned cursor iterates on tuples whose first element is the did of the document
   * that fullfil the query and the second is the value of the referenced attribute that this
   * document has.
   *
   * @param dbName
   * @param colName
   * @param attRef
   * @param values
   * @return
   */
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName,
      String colName, AttributeReference attRef, Collection<KvValue<?>> values);

  /**
   * Given a namespace and a cursor of dids, consumes the cursor and returns a new cursor that fetch
   * all iterated dids.
   *
   * @param dbName
   * @param colName
   * @param didCursor
   * @return
   */
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor);

  public Stream<CollectionInfo> getCollectionsInfo(String dbName);

  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException;

  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName);

  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException;

  @Override
  public void close();

  public void rollback();

}
