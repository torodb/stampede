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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.backend.d2r.ResultSetDocPartResult;
import com.torodb.backend.tables.MetaDocPartTable.DocPartTableFields;
import com.torodb.core.TableRef;
import com.torodb.core.TableRefFactory;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.cursors.IteratorCursor;
import com.torodb.core.d2r.DocPartResult;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KvValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.DSLContext;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.tuple.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class AbstractReadInterface implements ReadInterface {

  private final MetaDataReadInterface metaDataReadInterface;
  private final DataTypeProvider dataTypeProvider;
  private final ErrorHandler errorHandler;
  private final SqlHelper sqlHelper;
  private final TableRefFactory tableRefFactory;

  public AbstractReadInterface(MetaDataReadInterface metaDataReadInterface,
      DataTypeProvider dataTypeProvider,
      ErrorHandler errorHandler, SqlHelper sqlHelper, TableRefFactory tableRefFactory) {
    this.metaDataReadInterface = metaDataReadInterface;
    this.dataTypeProvider = dataTypeProvider;
    this.errorHandler = errorHandler;
    this.sqlHelper = sqlHelper;
    this.tableRefFactory = tableRefFactory;
  }

  @Override
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification =
      "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
  public Cursor<Integer> getCollectionDidsWithFieldEqualsTo(DSLContext dsl,
      MetaDatabase metaDatabase,
      MetaCollection metaCol, MetaDocPart metaDocPart, MetaField metaField, KvValue<?> value)
      throws SQLException {
    assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
    assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
    assert metaDocPart.getMetaFieldByIdentifier(metaField.getIdentifier()) != null;

    String statement = getReadCollectionDidsWithFieldEqualsToStatement(metaDatabase.getIdentifier(),
        metaDocPart.getIdentifier(), metaField.getIdentifier());
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(statement);
      sqlHelper.setPreparedStatementValue(preparedStatement, 1, metaField.getType(), value);
      return new DefaultDidCursor(errorHandler, preparedStatement.executeQuery());
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getReadCollectionDidsWithFieldEqualsToStatement(String schemaName,
      String rootTableName,
      String columnName);

  @Override
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification =
      "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
  public Cursor<Integer> getCollectionDidsWithFieldsIn(DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCol, MetaDocPart metaDocPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap)
      throws SQLException {
    assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
    assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
    assert valuesMultimap.keySet().stream().allMatch(metafield -> metaDocPart
        .getMetaFieldByIdentifier(metafield.getIdentifier()) != null);

    if (valuesMultimap.size() > 500) {
      @SuppressWarnings("checkstyle:LineLength")
      Stream<Entry<Long, List<Tuple2<Entry<MetaField, KvValue<?>>, Long>>>> valuesEntriesBatchStream =
          Seq.seq(valuesMultimap.entries().stream())
              .zipWithIndex()
              .groupBy(t -> t.v2 / 500)
              .entrySet()
              .stream();
      Stream<Stream<Entry<MetaField, KvValue<?>>>> valuesEntryBatchStreamOfStream =
          valuesEntriesBatchStream
              .map(e -> e.getValue()
                  .stream()
                  .map(se -> se.v1));
      Stream<Multimap<MetaField, KvValue<?>>> valuesMultimapBatchStream =
          valuesEntryBatchStreamOfStream
              .map(e -> toValuesMultimap(e));
      Stream<Cursor<Integer>> didCursorStream =
          valuesMultimapBatchStream
              .map(Unchecked.function(valuesMultimapBatch ->
                  getCollectionDidsWithFieldsInBatch(
                      dsl,
                      metaDatabase,
                      metaCol,
                      metaDocPart,
                      valuesMultimapBatch)));
      Stream<Integer> didStream = didCursorStream
          .flatMap(cursor -> cursor.getRemaining().stream());

      return new IteratorCursor<>(didStream.iterator());
    }

    return getCollectionDidsWithFieldsInBatch(dsl, metaDatabase, metaCol, metaDocPart,
        valuesMultimap);
  }

  private Multimap<MetaField, KvValue<?>> toValuesMultimap(
      Stream<Entry<MetaField, KvValue<?>>> valueEntryStream) {
    Multimap<MetaField, KvValue<?>> valuesMultimap = ArrayListMultimap.create();

    valueEntryStream.forEach(e -> valuesMultimap.put(e.getKey(), e.getValue()));

    return valuesMultimap;
  }

  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification =
      "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
  private Cursor<Integer> getCollectionDidsWithFieldsInBatch(DSLContext dsl,
      MetaDatabase metaDatabase,
      MetaCollection metaCol, MetaDocPart metaDocPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap)
      throws SQLException {
    @SuppressWarnings("checkstyle:LineLength")
    Provider<Stream<Map.Entry<MetaField, Collection<KvValue<?>>>>> valuesMultimapSortedStreamProvider =
        () -> valuesMultimap.asMap().entrySet().stream()
            .sorted((e1, e2) -> e1.getKey().getIdentifier().compareTo(e2.getKey().getIdentifier()));
    String statement = getReadCollectionDidsWithFieldInStatement(metaDatabase.getIdentifier(),
        metaDocPart.getIdentifier(), valuesMultimapSortedStreamProvider.get()
        .map(e -> new Tuple2<String, Integer>(e.getKey().getIdentifier(), e.getValue().size())));
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(statement);
      int parameterIndex = 1;
      Iterator<Map.Entry<MetaField, Collection<KvValue<?>>>> valuesMultimapSortedIterator =
          valuesMultimapSortedStreamProvider.get().iterator();
      while (valuesMultimapSortedIterator.hasNext()) {
        Map.Entry<MetaField, Collection<KvValue<?>>> valuesMultimapEntry =
            valuesMultimapSortedIterator.next();
        for (KvValue<?> value : valuesMultimapEntry.getValue()) {
          sqlHelper.setPreparedStatementValue(preparedStatement, parameterIndex, valuesMultimapEntry
              .getKey().getType(), value);
          parameterIndex++;
        }
      }
      return new DefaultDidCursor(errorHandler, preparedStatement.executeQuery());
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getReadCollectionDidsWithFieldInStatement(String schemaName,
      String rootTableName,
      Stream<Tuple2<String, Integer>> valuesCountList);

  @Override
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification = "ResultSet is wrapped in a Cursor<Tuple2<Integer, KVValue<?>>>. It's "
          + "iterated and closed in caller code")
  public Cursor<Tuple2<Integer, KvValue<?>>> getCollectionDidsAndProjectionWithFieldsIn(
      DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCol, MetaDocPart metaDocPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap)
      throws SQLException {
    assert metaDatabase.getMetaCollectionByIdentifier(metaCol.getIdentifier()) != null;
    assert metaCol.getMetaDocPartByIdentifier(metaDocPart.getIdentifier()) != null;
    assert valuesMultimap.keySet().stream().allMatch(metafield -> metaDocPart
        .getMetaFieldByIdentifier(metafield.getIdentifier()) != null);

    Stream<Tuple2<MetaField, Collection<KvValue<?>>>> valuesBatchStream =
        valuesMultimap.asMap().entrySet().stream()
            .map(e -> new Tuple2<MetaField, Collection<KvValue<?>>>(e.getKey(), e.getValue()));
    if (valuesMultimap.asMap().entrySet().stream().anyMatch(e -> e.getValue().size() > 500)) {
      valuesBatchStream = valuesBatchStream
          .flatMap(e -> Seq.seq(e.v2.stream())
              .zipWithIndex()
              .groupBy(t -> t.v2 / 500)
              .entrySet()
              .stream()
              .map(se -> toValuesMap(e.v1, se)));
    }
    Stream<Cursor<Tuple2<Integer, KvValue<?>>>> didProjectionCursorStream =
        valuesBatchStream
            .map(Unchecked.function(mapBatch ->
                getCollectionDidsAndProjectionWithFieldsInBatch(
                    dsl,
                    metaDatabase,
                    metaCol,
                    metaDocPart,
                    mapBatch.v1,
                    mapBatch.v2)));
    Stream<Tuple2<Integer, KvValue<?>>> didProjectionStream =
        didProjectionCursorStream
            .flatMap(cursor -> cursor.getRemaining().stream());

    return new IteratorCursor<>(didProjectionStream.iterator());
  }

  @SuppressWarnings("rawtypes")
  private Tuple2<MetaField, Collection<KvValue<?>>> toValuesMap(MetaField metaField,
      Entry<Long, List<Tuple2<KvValue<?>, Long>>> groupedValuesMap) {
    List<KvValue> collect = groupedValuesMap.getValue().stream()
        .map(e -> (KvValue) e.v1)
        .collect(Collectors.toList());

    return new Tuple2<MetaField, Collection<KvValue<?>>>(metaField, collect.stream()
        .map(e -> (KvValue<?>) e)
        .collect(Collectors.toList()));
  }

  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification = "ResultSet is wrapped in a Cursor<Tuple2<Integer, KVValue<?>>>. "
          + "It's iterated and closed in caller code")
  private Cursor<Tuple2<Integer, KvValue<?>>> getCollectionDidsAndProjectionWithFieldsInBatch(
      DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCol, MetaDocPart metaDocPart, MetaField metaField,
      Collection<KvValue<?>> values)
      throws SQLException {
    String statement = getReadCollectionDidsAndProjectionWithFieldInStatement(metaDatabase
        .getIdentifier(),
        metaDocPart.getIdentifier(), metaField.getIdentifier(), values.size());
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(statement);
      int parameterIndex = 1;
      for (KvValue<?> value : values) {
        sqlHelper.setPreparedStatementValue(preparedStatement, parameterIndex, metaField.getType(),
            value);
        parameterIndex++;
      }
      return new AbstractCursor<Tuple2<Integer, KvValue<?>>>(errorHandler, preparedStatement
          .executeQuery()) {
        @Override
        protected Tuple2<Integer, KvValue<?>> read(ResultSet resultSet) throws SQLException {
          return new Tuple2<>(
              resultSet.getInt(1),
              sqlHelper.getResultSetKvValue(
                metaField.getType(),
                dataTypeProvider.getDataType(metaField.getType()), resultSet, 2
              )
          );
        }
      };
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getReadCollectionDidsAndProjectionWithFieldInStatement(
      String schemaName,
      String rootTableName,
      String columnName, int valuesCount);

  @Override
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification =
      "ResultSet is wrapped in a Cursor<Integer>. It's iterated and closed in caller code")
  public Cursor<Integer> getAllCollectionDids(DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCollection)
      throws SQLException {

    MetaDocPart rootDocPart = metaCollection.getMetaDocPartByTableRef(tableRefFactory.createRoot());
    if (rootDocPart == null) {
      return new EmptyCursor<>();
    }

    String statement = getReadAllCollectionDidsStatement(metaDatabase.getIdentifier(), rootDocPart
        .getIdentifier());
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      PreparedStatement preparedStatement = connection.prepareStatement(statement);
      return new DefaultDidCursor(errorHandler, preparedStatement.executeQuery());
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getReadAllCollectionDidsStatement(String schemaName,
      String rootTableName);

  @Override
  public long countAll(
      @Nonnull DSLContext dsl,
      @Nonnull MetaDatabase database,
      @Nonnull MetaCollection collection
  ) {
    MetaDocPart rootDocPart = collection.getMetaDocPartByTableRef(tableRefFactory.createRoot());
    if (rootDocPart == null) {
      return 0;
    }
    String statement = getReadCountAllStatement(database.getIdentifier(), rootDocPart
        .getIdentifier());
    return sqlHelper.executeStatementWithResult(dsl, statement, Context.FETCH)
        .get(0).into(Long.class);
  }

  protected abstract String getReadCountAllStatement(String schema, String rootTableName);

  @Nonnull
  @Override
  public List<DocPartResult> getCollectionResultSets(@Nonnull DSLContext dsl,
      @Nonnull MetaDatabase metaDatabase, @Nonnull MetaCollection metaCollection,
      @Nonnull Cursor<Integer> didCursor, int maxSize) throws SQLException {
    Collection<Integer> dids = didCursor.getNextBatch(maxSize);
    return getCollectionResultSets(dsl, metaDatabase, metaCollection, dids);
  }

  @Override
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification =
      "ResultSet is wrapped in a DocPartResult. It's iterated and closed in caller code")
  public List<DocPartResult> getCollectionResultSets(DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCollection, Collection<Integer> dids) throws SQLException {
    ArrayList<DocPartResult> result = new ArrayList<>();
    Connection connection = dsl.configuration().connectionProvider().acquire();
    try {
      Iterator<? extends MetaDocPart> metaDocPartIterator = metaCollection
          .streamContainedMetaDocParts()
          .sorted(TableRefComparator.MetaDocPart.DESC)
          .iterator();
      while (metaDocPartIterator.hasNext()) {
        MetaDocPart metaDocPart = metaDocPartIterator.next();
        String statament = getDocPartStatament(metaDatabase, metaDocPart, dids);

        PreparedStatement preparedStatement = connection.prepareStatement(statament);
        result.add(new ResultSetDocPartResult(metaDataReadInterface, dataTypeProvider, errorHandler,
            metaDocPart, preparedStatement.executeQuery(), sqlHelper));
      }
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
    return result;
  }

  protected abstract String getDocPartStatament(MetaDatabase metaDatabase, MetaDocPart metaDocPart,
      Collection<Integer> dids);

  @Override
  public int getLastRowIdUsed(DSLContext dsl, MetaDatabase metaDatabase,
      MetaCollection metaCollection, MetaDocPart metaDocPart) {

    String statement = getLastRowIdUsedStatement(metaDatabase, metaDocPart);

    Connection connection = dsl.configuration().connectionProvider().acquire();
    try (PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
      try (ResultSet rs = preparedStatement.executeQuery()) {
        rs.next();
        int maxId = rs.getInt(1);
        if (rs.wasNull()) {
          return -1;
        }
        return maxId;
      }
    } catch (SQLException ex) {
      throw errorHandler.handleException(Context.FETCH, ex);
    } finally {
      dsl.configuration().connectionProvider().release(connection);
    }
  }

  protected abstract String getLastRowIdUsedStatement(MetaDatabase metaDatabase,
      MetaDocPart metaDocPart);

  protected String getPrimaryKeyColumnIdentifier(TableRef tableRef) {
    if (tableRef.isRoot()) {
      return DocPartTableFields.DID.fieldName;
    }
    return DocPartTableFields.RID.fieldName;
  }
}
