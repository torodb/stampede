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

import com.google.common.base.Preconditions;
import com.torodb.core.TableRef;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.exceptions.user.CollectionNotFoundException;
import com.torodb.core.exceptions.user.DatabaseNotFoundException;
import com.torodb.core.exceptions.user.UnsupportedCompoundIndexException;
import com.torodb.core.exceptions.user.UnsupportedUniqueIndexException;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.language.AttributeReference;
import com.torodb.core.transaction.RollbackException;
import com.torodb.core.transaction.WriteInternalTransaction;
import com.torodb.core.transaction.metainf.FieldIndexOrdering;
import com.torodb.core.transaction.metainf.FieldType;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.core.transaction.metainf.MetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaCollection;
import com.torodb.core.transaction.metainf.MutableMetaDatabase;
import com.torodb.core.transaction.metainf.MutableMetaIndex;
import com.torodb.core.transaction.metainf.MutableMetaSnapshot;
import com.torodb.kvdocument.values.KvDocument;
import com.torodb.kvdocument.values.KvValue;
import com.torodb.torod.IndexFieldInfo;
import com.torodb.torod.SharedWriteTorodTransaction;
import com.torodb.torod.pipeline.InsertPipeline;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 *
 */
public abstract class SqlWriteTorodTransaction<T extends WriteInternalTransaction<?>>
    extends SqlTorodTransaction<T>
    implements SharedWriteTorodTransaction {

  private final boolean concurrent;

  public SqlWriteTorodTransaction(SqlTorodConnection connection, boolean concurrent) {
    super(connection);

    this.concurrent = concurrent;
  }

  @Override
  public void insert(String db, String collection, Stream<KvDocument> documents) throws
      RollbackException, UserException {
    Preconditions.checkState(!isClosed());
    MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
    MutableMetaCollection metaCol = getOrCreateMetaCollection(metaDb, collection);

    //TODO: here we can not use a pipeline
    InsertPipeline pipeline = getConnection().getServer()
        .getInsertPipelineFactory()
        .createInsertPipeline(
            getConnection().getServer().getD2RTranslatorFactory(),
            metaDb,
            metaCol,
            getInternalTransaction().getBackendTransaction(),
            concurrent
        );
    pipeline.insert(documents);
  }

  @Override
  public void delete(String dbName, String colName, Cursor<Integer> cursor) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return;
    }

    getInternalTransaction().getBackendTransaction().deleteDids(db, col, cursor.getRemaining());
  }

  @Override
  public long deleteAll(String dbName, String colName) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }

    Collection<Integer> dids = getInternalTransaction().getBackendTransaction()
        .findAll(db, col)
        .asDidCursor()
        .getRemaining();
    getInternalTransaction().getBackendTransaction().deleteDids(db, col, dids);

    return dids.size();
  }

  @Override
  public long deleteByAttRef(String dbName, String colName, AttributeReference attRef,
      KvValue<?> value) {
    MetaDatabase db = getInternalTransaction().getMetaSnapshot().getMetaDatabaseByName(dbName);
    if (db == null) {
      return 0;
    }
    MetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return 0;
    }

    TableRef tableRef = extractTableRef(attRef);
    String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));

    MetaDocPart docPart = col.getMetaDocPartByTableRef(tableRef);
    if (docPart == null) {
      return 0;
    }

    MetaField field = docPart.getMetaFieldByNameAndType(lastKey, FieldType.from(value.getType()));
    if (field == null) {
      return 0;
    }

    Collection<Integer> dids = getInternalTransaction().getBackendTransaction()
        .findByField(db, col, docPart, field, value)
        .asDidCursor()
        .getRemaining();
    getInternalTransaction().getBackendTransaction().deleteDids(db, col, dids);

    return dids.size();
  }

  @Override
  public void dropCollection(String db, String collection) throws RollbackException, UserException {
    MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);
    MutableMetaCollection metaColl = getMetaCollectionOrThrowException(metaDb, collection);

    getInternalTransaction().getBackendTransaction().dropCollection(metaDb, metaColl);

    metaDb.removeMetaCollectionByName(collection);
  }

  @Override
  public void createCollection(String db, String collection)
      throws RollbackException, UserException {
    MutableMetaDatabase metaDb = getOrCreateMetaDatabase(db);
    getOrCreateMetaCollection(metaDb, collection);
  }

  @Override
  public void dropDatabase(String db) throws RollbackException, UserException {
    MutableMetaDatabase metaDb = getMetaDatabaseOrThrowException(db);

    getInternalTransaction().getBackendTransaction().dropDatabase(metaDb);

    getInternalTransaction().getMetaSnapshot().removeMetaDatabaseByName(db);
  }

  @Override
  public boolean createIndex(String dbName, String colName, String indexName,
      List<IndexFieldInfo> fields, boolean unique) throws UserException {
    if (fields.size() > 1) {
      throw new UnsupportedCompoundIndexException(dbName, colName, indexName);
    }
      
    MutableMetaDatabase metaDb = getOrCreateMetaDatabase(dbName);
    MutableMetaCollection metaColl = getOrCreateMetaCollection(metaDb, colName);

    List<Tuple3<TableRef, String, FieldIndexOrdering>> indexFieldDefs = new ArrayList<>(fields
        .size());
    for (IndexFieldInfo field : fields) {
      AttributeReference attRef = field.getAttributeReference();
      FieldIndexOrdering ordering = field.isAscending() ? FieldIndexOrdering.ASC :
          FieldIndexOrdering.DESC;
      TableRef tableRef = extractTableRef(attRef);
      String lastKey = extractKeyName(attRef.getKeys().get(attRef.getKeys().size() - 1));
      indexFieldDefs.add(new Tuple3<>(tableRef, lastKey, ordering));
    }

    if (unique) {
      TableRef anyIndexTableRef = indexFieldDefs.stream()
          .findAny().get().v1();
      boolean isUniqueIndexWithMutlipleTableRefs = indexFieldDefs.stream()
          .anyMatch(t -> !t.v1().equals(anyIndexTableRef));

      if (isUniqueIndexWithMutlipleTableRefs) {
        throw new UnsupportedUniqueIndexException(dbName, colName, indexName);
      }
    }

    boolean indexExists = metaColl.streamContainedMetaIndexes()
        .anyMatch(index -> index.getName().equals(indexName) || (index.isUnique() == unique
            && index.size() == indexFieldDefs.size() && Seq.seq(index.iteratorFields())
            .allMatch(indexField -> {
              Tuple3<TableRef, String, FieldIndexOrdering> indexFieldDef =
                  indexFieldDefs.get(indexField.getPosition());
              return indexFieldDef != null && indexFieldDef.v1().equals(indexField.getTableRef())
                  && indexFieldDef.v2().equals(indexField.getName()) && indexFieldDef.v3()
                  == indexField.getOrdering();
            })));

    if (!indexExists) {
      MutableMetaIndex metaIndex = metaColl.addMetaIndex(indexName, unique);
      for (Tuple3<TableRef, String, FieldIndexOrdering> indexFieldDef : indexFieldDefs) {
        metaIndex.addMetaIndexField(indexFieldDef.v1(), indexFieldDef.v2(), indexFieldDef.v3());
      }
      getInternalTransaction().getBackendTransaction().createIndex(metaDb, metaColl, metaIndex);
    }

    return !indexExists;
  }

  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
      justification = "Findbugs thinks MutableMetaCollection#removeMetaIndexByName"
      + "has no side effect")
  @Override
  public boolean dropIndex(String dbName, String colName, String indexName) {
    MutableMetaDatabase db = getInternalTransaction().getMetaSnapshot()
        .getMetaDatabaseByName(dbName);
    if (db == null) {
      return false;
    }
    MutableMetaCollection col = db.getMetaCollectionByName(colName);
    if (col == null) {
      return false;
    }
    MetaIndex index = col.getMetaIndexByName(indexName);
    if (index == null) {
      return false;
    }
    col.removeMetaIndexByName(indexName);

    getInternalTransaction().getBackendTransaction().dropIndex(db, col, index);

    return true;
  }

  @Nonnull
  protected MutableMetaDatabase getOrCreateMetaDatabase(String dbName) {
    MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
    MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

    if (metaDb == null) {
      metaDb = createMetaDatabase(dbName);
    }
    return metaDb;
  }

  private MutableMetaDatabase createMetaDatabase(String dbName) {
    Preconditions.checkState(!isClosed());
    MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
    MutableMetaDatabase metaDb = metaSnapshot.addMetaDatabase(
        dbName,
        getConnection().getServer().getIdentifierFactory().toDatabaseIdentifier(
            metaSnapshot, dbName)
    );
    getInternalTransaction().getBackendTransaction().addDatabase(metaDb);
    return metaDb;
  }

  protected MutableMetaCollection getOrCreateMetaCollection(@Nonnull MutableMetaDatabase metaDb,
      String colName) {
    MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

    if (metaCol == null) {
      metaCol = createMetaCollection(metaDb, colName);
    }
    return metaCol;
  }

  protected MutableMetaCollection createMetaCollection(MutableMetaDatabase metaDb, String colName) {
    MutableMetaCollection metaCol;
    Preconditions.checkState(!isClosed());
    metaCol = metaDb.addMetaCollection(
        colName,
        getConnection().getServer().getIdentifierFactory().toCollectionIdentifier(
            getInternalTransaction().getMetaSnapshot(), metaDb.getName(), colName)
    );
    getInternalTransaction().getBackendTransaction().addCollection(metaDb, metaCol);
    return metaCol;
  }

  @Nonnull
  protected MutableMetaDatabase getMetaDatabaseOrThrowException(@Nonnull String dbName) throws
      DatabaseNotFoundException {
    MutableMetaSnapshot metaSnapshot = getInternalTransaction().getMetaSnapshot();
    MutableMetaDatabase metaDb = metaSnapshot.getMetaDatabaseByName(dbName);

    if (metaDb == null) {
      throw new DatabaseNotFoundException(dbName);
    }
    return metaDb;
  }

  @Nonnull
  protected MutableMetaCollection getMetaCollectionOrThrowException(
      @Nonnull MutableMetaDatabase metaDb, @Nonnull String colName) throws
      CollectionNotFoundException {
    MutableMetaCollection metaCol = metaDb.getMetaCollectionByName(colName);

    if (metaCol == null) {
      throw new CollectionNotFoundException(metaDb.getName(), colName);
    }
    return metaCol;
  }

  @Override
  public void commit() throws RollbackException, UserException {
    getInternalTransaction().commit();
  }

}
