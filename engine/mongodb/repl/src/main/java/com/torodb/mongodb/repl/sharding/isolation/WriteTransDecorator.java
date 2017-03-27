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

package com.torodb.mongodb.repl.sharding.isolation;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SharedWriteTorodTransaction;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.cursors.TorodCursor;

import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:LineLength")
public abstract class WriteTransDecorator<D extends SharedWriteTorodTransaction, C extends TorodConnection>
    extends TransDecorator<D, C>
    implements SharedWriteTorodTransaction {

  public WriteTransDecorator(C connection, D decorated) {
    super(connection, decorated);
  }

  @Override
  public void insert(String dbName, String colName, Stream<KvDocument> documents) throws
      RollbackException, UserException {
    getDecorated().insert(dbName, colName, documents);
  }

  @Override
  public void delete(String dbName, String colName, List<ToroDocument> candidates) {
    getDecorated().delete(dbName, colName, candidates);
  }

  @Override
  public void delete(String dbName, String colName, TorodCursor cursor) {
    getDecorated().delete(dbName, colName, cursor);
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    getDecorated().delete(dbName, colName, cursor);
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    return getDecorated().deleteAll(dbName, colName);
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return getDecorated().deleteByAttRef(dbName, colName, attRef, value);
  }

  @Override
  public void dropCollection(String db, String collection) throws RollbackException, UserException {
    getDecorated().dropCollection(db, collection);
  }

  @Override
  public void createCollection(String db, String collection) throws RollbackException, UserException {
    getDecorated().createCollection(db, collection);
  }

  @Override
  public void dropDatabase(String db) throws RollbackException, UserException {
    getDecorated().dropDatabase(db);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UserException {
    return getDecorated().createIndex(dbName, colName, indexName, fields, unique);
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) {
    return getDecorated().dropIndex(dbName, colName, indexName);
  }

  @Override
  public void commit() throws RollbackException, UserException {
    getDecorated().commit();
  }

}
