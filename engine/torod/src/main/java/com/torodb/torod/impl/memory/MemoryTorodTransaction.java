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
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.cursors.TransformCursor;
import com.torodb.core.document.ToroDocument;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.util.AttributeRefKvDocResolver;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodTransaction;
import com.torodb.torod.cursors.DocTorodCursor;
import com.torodb.torod.cursors.TorodCursor;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.Json;

/**
 *
 */
public abstract class MemoryTorodTransaction implements TorodTransaction {

  private boolean closed = false;
  private final MemoryTorodConnection connection;

  public MemoryTorodTransaction(MemoryTorodConnection connection) {
    this.connection = connection;
  }

  protected abstract MemoryData.MdTransaction getTransaction();

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public MemoryTorodConnection getConnection() {
    return connection;
  }

  @Override
  public boolean existsDatabase(String dbName) {
    return getTransaction().existsDatabase(dbName);
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    return getTransaction().existsCollection(dbName, colName);
  }

  @Override
  public List<String> getDatabases() {
    return getTransaction().streamDbs().collect(Collectors.toList());
  }

  @Override
  public long countAll(String dbName, String colName) {
    return getTransaction().streamCollection(dbName, colName).count();
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    return createCursor(getTransaction().streamCollection(dbName, colName));
  }

  Stream<ToroDocument> streamByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return getTransaction().streamCollection(dbName, colName)
        .filter(doc -> {
          Optional<KvValue<?>> resolved = AttributeRefKvDocResolver.resolve(
              attRef, doc.getRoot());
          return resolved.isPresent() && value.equals(resolved.get());
        });
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    return createCursor(streamByAttRef(dbName, colName, attRef, value));
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    return createCursor(getTransaction().streamCollection(dbName, colName)
            .filter(doc -> {
              Optional<KvValue<?>> resolved = AttributeRefKvDocResolver.resolve(
                  attRef, doc.getRoot());
              return resolved.isPresent() && values.contains(resolved.get());
            })
    );
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName,
      String colName, AttributeReference attRef, Collection<KvValue<?>> values) {
    Cursor<ToroDocument> docCursor = findByAttRefIn(dbName, colName, attRef, values)
        .asDocCursor();
    return new TransformCursor<>(docCursor, (toroDoc) -> {
      Optional<KvValue<?>> resolved = AttributeRefKvDocResolver.resolve(attRef, toroDoc.getRoot());
      assert resolved.isPresent();
      return new Tuple2<>(toroDoc.getId(), resolved.get());
    });
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    Map<Integer, KvDocument> colData = getTransaction().data.get(dbName, colName);
    return createCursor(didCursor.getRemaining().stream()
        .map(did -> new Tuple2<>(did, colData.get(did)))
        .filter(tuple -> tuple.v2 != null)
        .map(tuple -> new ToroDocument(tuple.v1, tuple.v2))
    );
  }

  private TorodCursor createCursor(Stream<ToroDocument> docsStream) {
    return new DocTorodCursor(new IteratorCursor<>(docsStream.iterator()));
  }

  @Override
  public long getDatabaseSize(String dbName) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    throw new UnsupportedOperationException("Not supported yet."); //TODO: Implement when necessary
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    return getTransaction().data.row(dbName).keySet().stream()
        .map(colName -> getCollectionInfoPrivate(colName));
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    if (!getTransaction().data.contains(dbName, colName)) {
      throw new CollectionNotFoundException(dbName, colName);
    }
    return getCollectionInfoPrivate(colName);
  }

  private CollectionInfo getCollectionInfoPrivate(String colName) {
    return new CollectionInfo(colName, Json.createObjectBuilder().build());
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    Map<String, IndexInfo> indexesOnTable = getTransaction().getIndexes()
        .get(dbName, colName);
    if (indexesOnTable == null) {
      return Stream.empty();
    }
    return indexesOnTable.values().stream();
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName)
      throws IndexNotFoundException {
    return getIndexesInfo(dbName, colName)
        .filter(index -> index.getName().equals(idxName))
        .findAny().orElseThrow(() ->
            new IndexNotFoundException(dbName, colName, idxName));
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      getTransaction().close();
      connection.onTransactionClosed(this);
    }
  }

}
