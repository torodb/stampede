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

package com.torodb.backend;

import com.google.common.base.Preconditions;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.tables.KvTable;
import com.torodb.backend.tables.MetaDocPartTable;
import com.torodb.backend.tables.records.KvRecord;
import com.torodb.backend.tables.records.MetaDatabaseRecord;
import com.torodb.core.TableRef;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaIdentifiedDocPartIndex;
import com.torodb.core.transaction.metainf.MetaIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class AbstractMetaDataReadInterface implements MetaDataReadInterface {

  private final MetaDocPartTable<?, ?> metaDocPartTable;
  private final SqlHelper sqlHelper;

  @Inject
  public AbstractMetaDataReadInterface(MetaDocPartTable<?, ?> metaDocPartTable,
      SqlHelper sqlHelper) {
    this.metaDocPartTable = metaDocPartTable;
    this.sqlHelper = sqlHelper;
  }

  @Override
  public long getDatabaseSize(
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase database
  ) {
    String statement = getReadSchemaSizeStatement(database.getIdentifier());
    Result<Record> result = sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH,
        ps -> {
          ps.setString(1, database.getName());
        }
    );

    if (result.isEmpty()) {
      return 0;
    }

    Long resultSize = result.get(0).into(Long.class);

    if (resultSize == null) {
      return 0;
    }

    return resultSize;
  }

  protected abstract String getReadSchemaSizeStatement(String databaseName);

  @Override
  public long getCollectionSize(
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection
  ) {
    String statement = getReadCollectionSizeStatement();
    return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH,
        ps -> {
          ps.setString(1, database.getName());
          ps.setString(2, database.getIdentifier());
          ps.setString(3, collection.getName());
        })
        .get(0)
        .into(Long.class);
  }

  protected abstract String getReadCollectionSizeStatement();

  @Override
  public long getDocumentsSize(
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection
  ) {
    String statement = getReadDocumentsSizeStatement();
    return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH,
        ps -> {
          ps.setString(1, database.getName());
          ps.setString(2, database.getIdentifier());
          ps.setString(3, collection.getName());
        })
        .get(0)
        .into(Long.class);
  }

  protected abstract String getReadDocumentsSizeStatement();

  @Override
  public Long getIndexSize(
      @Nonnull DSLContext dsl, @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection, @Nonnull String indexName) {
    long result = 0;
    MetaIndex index = collection.getMetaIndexByName(indexName);
    Iterator<TableRef> tableRefIterator = index.streamTableRefs().iterator();
    while (tableRefIterator.hasNext()) {
      TableRef tableRef = tableRefIterator.next();
      MetaDocPart docPart = collection.getMetaDocPartByTableRef(tableRef);
      Iterator<? extends MetaIdentifiedDocPartIndex> docPartIndexIterator = docPart.streamIndexes()
          .iterator();
      while (docPartIndexIterator.hasNext()) {
        MetaIdentifiedDocPartIndex docPartIndex = docPartIndexIterator.next();
        if (index.isCompatible(docPart, docPartIndex)) {
          long relatedIndexCount = collection.streamContainedMetaIndexes()
              .filter(i -> i.isCompatible(docPart, docPartIndex)).count();
          String statement = getReadIndexSizeStatement(database.getIdentifier(),
              docPart.getIdentifier(), docPartIndex.getIdentifier());
          result += sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH)
              .get(0).into(Long.class) / relatedIndexCount;
        }
      }
    }
    return result;
  }

  protected abstract String getReadIndexSizeStatement(
      String schemaName, String tableName, String indexName);

  @Override
  public Collection<InternalField<?>> getInternalFields(MetaDocPart metaDocPart) {
    TableRef tableRef = metaDocPart.getTableRef();
    return getInternalFields(tableRef);
  }

  @Override
  public Collection<InternalField<?>> getInternalFields(TableRef tableRef) {
    if (tableRef.isRoot()) {
      return metaDocPartTable.ROOT_FIELDS;
    } else if (tableRef.getParent().get().isRoot()) {
      return metaDocPartTable.FIRST_FIELDS;
    }
    return metaDocPartTable.FIELDS;
  }

  @Override
  public Collection<InternalField<?>> getPrimaryKeyInternalFields(TableRef tableRef) {
    if (tableRef.isRoot()) {
      return metaDocPartTable.PRIMARY_KEY_ROOT_FIELDS;
    } else if (tableRef.getParent().get().isRoot()) {
      return metaDocPartTable.PRIMARY_KEY_FIRST_FIELDS;
    }
    return metaDocPartTable.PRIMARY_KEY_FIELDS;
  }

  @Override
  public Collection<InternalField<?>> getReferenceInternalFields(TableRef tableRef) {
    Preconditions.checkArgument(!tableRef.isRoot());
    if (tableRef.getParent().get().isRoot()) {
      return metaDocPartTable.REFERENCE_FIRST_FIELDS;
    }
    return metaDocPartTable.REFERENCE_FIELDS;
  }

  @Override
  public Collection<InternalField<?>> getForeignInternalFields(TableRef tableRef) {
    Preconditions.checkArgument(!tableRef.isRoot());
    TableRef parentTableRef = tableRef.getParent().get();
    if (parentTableRef.isRoot()) {
      return metaDocPartTable.FOREIGN_ROOT_FIELDS;
    } else if (parentTableRef.getParent().get().isRoot()) {
      return metaDocPartTable.FOREIGN_FIRST_FIELDS;
    }
    return metaDocPartTable.FOREIGN_FIELDS;
  }

  @Override
  public Collection<InternalField<?>> getReadInternalFields(MetaDocPart metaDocPart) {
    TableRef tableRef = metaDocPart.getTableRef();
    return getReadInternalFields(tableRef);
  }

  @Override
  public Collection<InternalField<?>> getReadInternalFields(TableRef tableRef) {
    if (tableRef.isRoot()) {
      return metaDocPartTable.READ_ROOT_FIELDS;
    } else if (tableRef.getParent().get().isRoot()) {
      return metaDocPartTable.READ_FIRST_FIELDS;
    }
    return metaDocPartTable.READ_FIELDS;
  }

  @Override
  public Optional<String> readKv(DSLContext dsl, MetaInfoKey key) {
    KvTable<KvRecord> kvTable = getKvTable();
    Condition c = kvTable.KEY.eq(key.getKeyName());

    return dsl.select(kvTable.VALUE)
        .from(kvTable)
        .where(c)
        .fetchOptional()
        .map(Record1::value1);
  }

  @Override
  public Stream<MetaDatabaseRecord> readMetaDatabaseTable(DSLContext dsl) {
    return dsl.selectFrom(getMetaDatabaseTable())
        .fetchStream();
  }
}
