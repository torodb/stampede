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
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.cursors.TorodCursor;

import java.util.List;
import java.util.stream.Stream;

/**
 *
 */
public interface SharedWriteTorodTransaction extends TorodTransaction {

  public void insert(String dbName, String colName, Stream<KvDocument> documents) throws
      RollbackException, UserException;

  public default void delete(String dbName, String colName, List<ToroDocument> candidates) {
    delete(dbName, colName, new IteratorCursor<>(candidates.stream().map(ToroDocument::getId)
        .iterator()));
  }

  public default void delete(String dbName, String colName, TorodCursor cursor) {
    delete(dbName, colName, cursor.asDidCursor());
  }

  public void delete(String dbName, String colName, Cursor<Integer> cursor);

  public long deleteAll(String dbName, String colName);

  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value);

  public void dropCollection(String db, String collection) throws RollbackException, UserException;

  public void createCollection(String db, String collection)
      throws RollbackException, UserException;

  public void dropDatabase(String db) throws RollbackException, UserException;

  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UserException;

  public boolean dropIndex(String dbName, String colName, String indexName);

  public void commit() throws RollbackException, UserException;

}
