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

package com.torodb.torod.impl.sql;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.IndexNotFoundException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.language.AttributeReference.Key;
import com.torodb.core.language.AttributeReference.ObjectKey;
import com.torodb.core.transaction.InternalTransaction;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.CollectionInfo;
import com.torodb.torod.IndexInfo;
import com.torodb.torod.TorodTransaction;
import com.torodb.torod.cursors.EmptyTorodCursor;
import com.torodb.torod.cursors.TorodCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.Json;

/**
 *
 */
public abstract class SqlTorodTransaction<T extends InternalTransaction>
    implements TorodTransaction {

  private static final Logger LOGGER = LogManager.getLogger(SqlTorodTransaction.class);
  private boolean closed = false;
  private final SqlTorodConnection connection;
  private final T internalTransaction;

  public SqlTorodTransaction(SqlTorodConnection connection) {
    this.connection = connection;
    this.internalTransaction = createInternalTransaction(connection);
  }

  protected abstract T createInternalTransaction(SqlTorodConnection connection);

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public final SqlTorodConnection getConnection() {
    return connection;
  }

  protected T getInternalTransaction() {
    return internalTransaction;
  }

  @Override
  public boolean existsDatabase(String dbName) {
    MetaDatabase metaDb = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    return metaDb != null;
  }

  @Override
  public boolean existsCollection(String dbName, String colName) {
    MetaDatabase metaDb = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    return metaDb != null && metaDb.getMetaCollectionByName(colName) != null;
  }

  @Override
  public List<String> getDatabases() {
    return getInternalTransaction().getMetaSnapshot().streamMetaDatabases()
        .map(metaDb -> metaDb.getName()).collect(Collectors.toList());
  }

  @Override
  public long getDatabaseSize(String dbName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0L;
    }
    return getInternalTransaction().getBackendTransaction().getDatabaseSize(db);
  }

  @Override
  public long countAll(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getInternalTransaction().getBackendTransaction().countAll(db, col);
  }

  @Override
  public long getCollectionSize(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getInternalTransaction().getBackendTransaction().getCollectionSize(db, col);
  }

  @Override
  public long getDocumentsSize(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }
    return getInternalTransaction().getBackendTransaction().getDocumentsSize(db, col);
  }

  @Override
  public TorodCursor findAll(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    return toToroCursor(getInternalTransaction()
        .getBackendTransaction()
        .findAll(db, col)
    );
  }

  @Override
  public TorodCursor findByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
    if (field == null) {
      LOGGER.trace("Field " + dbName + '.' + colName + '.' + ref + '.' + lastKey
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    return toToroCursor(getInternalTransaction()
        .getBackendTransaction()
        .findByField(db, col, docPart, field, value)
    );
  }

  @Override
  public TorodCursor findByAttRefIn(String dbName, String colName, AttributeReference attRef,
      Collection<KvValue<?>> values) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    if (values.isEmpty()) {
      LOGGER.trace(
          "An empty list of values have been given as in condition. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }

    Multimap<MetaField, KvValue<?>> valuesMap = ArrayListMultimap.create();
    for (KvValue<?> value : values) {
      MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
      if (field != null) {
        valuesMap.put(field, value);
      }
    }
    return toToroCursor(getInternalTransaction()
        .getBackendTransaction()
        .findByFieldIn(db, col, docPart, valuesMap)
    );
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByAttRefInProjection(String dbName,
      String colName, AttributeReference attRef, Collection<KvValue<?>> values) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }
    if (values.isEmpty()) {
      LOGGER.trace(
          "An empty list of values have been given as in condition. An empty cursor is returned");
      return new EmptyCursor<>();
    }

    TableRef ref = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(ref);
    if (docPart == null) {
      LOGGER.trace("DocPart " + dbName + '.' + colName + '.' + ref
          + " does not exist. An empty cursor is returned");
      return new EmptyCursor<>();
    }

    Multimap<MetaField, KvValue<?>> valuesMap = ArrayListMultimap.create();
    for (KvValue<?> value : values) {
      MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
      if (field != null) {
        valuesMap.put(field, value);
      }
    }
    return getInternalTransaction().getBackendTransaction()
        .findByFieldInProjection(db, col, docPart, valuesMap);
  }

  @Override
  public TorodCursor fetch(String dbName, String colName, Cursor<Integer> didCursor) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      LOGGER.trace("Db with name " + dbName + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      LOGGER.trace("Collection " + dbName + '.' + colName
          + " does not exist. An empty cursor is returned");
      return new EmptyTorodCursor();
    }
    return toToroCursor(getInternalTransaction()
        .getBackendTransaction()
        .fetch(db, col, didCursor)
    );
  }

  private TorodCursor toToroCursor(BackendCursor backendCursor) {
    R2DTranslator r2dTrans = getConnection().getServer().getR2DTranslator();
    return new LazyTorodCursor(r2dTrans, backendCursor);
  }

  @Override
  public Stream<CollectionInfo> getCollectionsInfo(String dbName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return Stream.empty();
    }

    return db.streamMetaCollections()
        .map(metaCol -> new CollectionInfo(metaCol.getName(), Json.createObjectBuilder().build()));
  }

  @Override
  public CollectionInfo getCollectionInfo(String dbName, String colName) throws
      CollectionNotFoundException {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new CollectionNotFoundException(dbName, colName);
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new CollectionNotFoundException(dbName, colName);
    }

    return new CollectionInfo(db.getMetaCollectionByName(colName).getName(), Json
        .createObjectBuilder().build());
  }

  @Override
  public Stream<IndexInfo> getIndexesInfo(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return Stream.empty();
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return Stream.empty();
    }

    return col.streamContainedMetaIndexes()
        .map(metaIdx -> createIndexInfo(metaIdx));
  }

  @Override
  public IndexInfo getIndexInfo(String dbName, String colName, String idxName) throws
      IndexNotFoundException {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }
    MetaIndex idx = col.getMetaIndexByName(idxName);
    if (idx == null) {
      throw new IndexNotFoundException(dbName, colName, idxName);
    }

    return createIndexInfo(idx);
  }

  protected IndexInfo createIndexInfo(MetaIndex metaIndex) {
    IndexInfo.Builder indexInfoBuilder = new IndexInfo.Builder(metaIndex.getName(), metaIndex
        .isUnique());

    metaIndex.iteratorFields()
        .forEachRemaining(metaIndexField ->
            indexInfoBuilder.addField(
                getAttrivuteReference(metaIndexField.getTableRef(), metaIndexField.getName()),
                metaIndexField.getOrdering().isAscending()));

    return indexInfoBuilder.build();
  }

  protected AttributeReference getAttrivuteReference(TableRef tableRef, String name) {
    AttributeReference.Builder attributeReferenceBuilder = new AttributeReference.Builder();

    while (!tableRef.isRoot()) {
      attributeReferenceBuilder.addObjectKeyAsFirst(tableRef.getName());
      tableRef = tableRef.getParent().get();
    }

    attributeReferenceBuilder.addObjectKey(name);

    return attributeReferenceBuilder.build();
  }

  protected TableRef extractTableRef(AttributeReference attRef) {
    TableRefFactory tableRefFactory = getConnection().getServer().getTableRefFactory();
    TableRef ref = tableRefFactory.createRoot();

    if (attRef.getKeys().isEmpty()) {
      throw new IllegalArgumentException("The empty attribute reference is not valid");
    }
    if (attRef.getKeys().size() > 1) {
      List<Key<?>> keys = attRef.getKeys();
      List<Key<?>> tableKeys = keys.subList(0, keys.size() - 1);
      for (Key<?> key : tableKeys) {
        ref = tableRefFactory.createChild(ref, extractKeyName(key));
      }
    }
    return ref;
  }

  protected String extractKeyName(Key<?> key) {
    if (key instanceof ObjectKey) {
      return ((ObjectKey) key).getKey();
    } else {
      throw new IllegalArgumentException("Keys whose type is not object are not valid");
    }
  }

  @Override
  public void rollback() {
    getInternalTransaction().rollback();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      getInternalTransaction().close();
      connection.onTransactionClosed(this);
    }
  }

}
