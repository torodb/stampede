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

package com.torodb.torod.impl.memory;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.ExclusiveWriteTorodTransaction;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.impl.memory.MemoryData.MdTransaction;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
public class MemoryWriteTorodTransaction extends MemoryTorodTransaction implements
    ExclusiveWriteTorodTransaction {

  private final MemoryData.MdWriteTransaction trans;

  public MemoryWriteTorodTransaction(MemoryTorodConnection connection) {
    super(connection);
    this.trans = connection.getServer().getData().openWriteTransaction();
  }

  @Override
  protected MdTransaction getTransaction() {
    return trans;
  }

  @Override
  public void insert(String db, String collection, Stream<KvDocument> documents) throws
      RollbackException, UserException {
    trans.insert(db, collection, documents);
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    long count = trans.streamCollection(dbName, colName).count();
    trans.deleteAll(dbName, colName);
    return count;
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return trans.delete(dbName, colName, streamByAttRef(dbName, colName, attRef, value).map(
        ToroDocument::getId));
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    trans.delete(dbName, colName, cursor.getRemaining().stream());
  }

  @Override
  public void dropCollection(String dbName, String colName)
      throws RollbackException, UserException {
    trans.dropCollection(dbName, colName);
  }

  @Override
  public void renameCollection(String fromDb, String fromCollection, String toDb,
      String toCollection)
      throws RollbackException, UserException {
    trans.renameCollection(fromDb, fromCollection, toDb, toCollection);
  }

  @Override
  public void createCollection(String dbName, String colName) throws RollbackException,
      UserException {
    trans.createCollection(dbName, colName);
  }

  @Override
  public void dropDatabase(String dbName) throws RollbackException, UserException {
    trans.dropDatabase(dbName);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) {
    return false;
  }

  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) {
    Map<String, IndexInfo> indexesOnTable = getTransaction().getIndexes()
        .get(dbName, colName);
    if (indexesOnTable == null) {
      return false;
    }
    return indexesOnTable.remove(indexName) != null;
  }

  @Override
  public void rollback() {
    trans.rollback();
  }

  @Override
  public void commit() throws RollbackException, UserException {
    trans.commit();
  }

}
