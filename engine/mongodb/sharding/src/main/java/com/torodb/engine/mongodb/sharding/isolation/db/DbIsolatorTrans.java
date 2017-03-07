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

package com.torodb.engine.mongodb.sharding.isolation.db;

import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.engine.mongodb.sharding.isolation.TransDecorator;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodTransaction;
import com.torodb.torod.cursors.TorodCursor;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbIsolatorTrans<D extends TorodTransaction>
    extends TransDecorator<D, DbIsolatorConn> {
  
  public DbIsolatorTrans(DbIsolatorConn connection, D decorated) {
    super(connection, decorated);
  }

  final String convertDatabaseName(String dbName) {
    return getConnection().convertDatabaseName(dbName);
  }

  final String convertIndexName(String indexName) {
    return getConnection().convertIndexName(indexName);
  }

  final boolean isVisibleDatabase(String dbName) {
    return getConnection().isVisibleDatabase(dbName);
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException {
    return super.getIndexInfo(convertDatabaseName(dbName), colName, idxName);
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    return super.getIndexesInfo(convertDatabaseName(dbName), colName);
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    return super.getCollectionInfo(convertDatabaseName(dbName), colName);
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    return super.getCollectionsInfo(convertDatabaseName(dbName));
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    return super.fetch(convertDatabaseName(dbName), colName, didCursor);
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName, String colName,
      AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return super.findByAttRefInProjection(convertDatabaseName(dbName), colName, attRef, values);
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return super.findByAttRefIn(convertDatabaseName(dbName), colName, attRef, values);
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return super.findByAttRef(convertDatabaseName(dbName), colName, attRef, value);
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    return super.findAll(convertDatabaseName(dbName), colName);
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    return super.getDocumentsSize(convertDatabaseName(dbName), colName);
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    return super.getCollectionSize(convertDatabaseName(dbName), colName);
  }

  @Override
  public long countAll(String dbName, String colName) {
    return super.countAll(convertDatabaseName(dbName), colName);
  }

  @Override
  public long getDatabaseSize(String dbName) {
    return super.getDatabaseSize(convertDatabaseName(dbName));
  }

  @Override
  public List<String> getDatabases() {
    return super.getDatabases().stream()
        .filter(this::isVisibleDatabase)
        .collect(Collectors.toList());
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    return super.existsCollection(convertDatabaseName(dbName), colName);
  }

  @Override
  public boolean existsDatabase(String dbName) {
    return super.existsDatabase(convertDatabaseName(dbName));
  }


}
