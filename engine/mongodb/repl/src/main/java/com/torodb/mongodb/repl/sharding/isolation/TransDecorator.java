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
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodTransaction;
import com.torodb.torod.cursors.TorodCursor;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public abstract class TransDecorator<D extends TorodTransaction, C extends TorodConnection>
    implements TorodTransaction {

  private final C connection;
  private final D decorated;

  public TransDecorator(C connection, D decorated) {
    this.connection = connection;
    this.decorated = decorated;
  }

  protected D getDecorated() {
    return decorated;
  }

  @Override
  public boolean isClosed() {
    return decorated.isClosed();
  }

  @Override
  public C getConnection() {
    return connection;
  }

  @Override
  public boolean existsDatabase(String dbName) {
    return decorated.existsDatabase(dbName);
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    return decorated.existsCollection(dbName, colName);
  }

  @Override
  public List<String> getDatabases() {
    return decorated.getDatabases();
  }

  @Override
  public long getDatabaseSize(String dbName) {
    return decorated.getDatabaseSize(dbName);
  }

  @Override
  public long countAll(String dbName, String colName) {
    return decorated.countAll(dbName, colName);
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    return decorated.getCollectionSize(dbName, colName);
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    return decorated.getDocumentsSize(dbName, colName);
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    return decorated.findAll(dbName, colName);
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return decorated.findByAttRef(dbName, colName, attRef, value);
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return decorated.findByAttRefIn(dbName, colName, attRef, values);
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName, String colName,
      AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return decorated.findByAttRefInProjection(dbName, colName, attRef, values);
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    return decorated.fetch(dbName, colName, didCursor);
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    return decorated.getCollectionsInfo(dbName);
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    return decorated.getCollectionInfo(dbName, colName);
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return decorated.getIndexesInfo(dbName, colName);
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException {
    return decorated.getIndexInfo(dbName, colName, idxName);
  }

  @Override
  public void close() {
    decorated.close();
  }

  @Override
  public void rollback() {
    decorated.rollback();
  }

}
