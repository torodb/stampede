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

import com.google.common.collect.Multimap;
import com.torodb.backend.ErrorHandler.Context;
import com.torodb.core.backend.BackendCursor;
import com.torodb.core.backend.BackendTransaction;
import com.torodb.core.backend.EmptyBackendCursor;
import com.torodb.core.backend.MetaInfoKey;
import com.torodb.core.cursors.Cursor;
import com.torodb.core.cursors.EmptyCursor;
import com.torodb.core.exceptions.InvalidDatabaseException;
import com.torodb.core.transaction.metainf.MetaCollection;
import com.torodb.core.transaction.metainf.MetaDatabase;
import com.torodb.core.transaction.metainf.MetaDocPart;
import com.torodb.core.transaction.metainf.MetaField;
import com.torodb.kvdocument.values.KvValue;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

/**
 *
 */
public abstract class BackendTransactionImpl implements BackendTransaction {

  private boolean closed = false;
  private final Connection connection;
  private final DSLContext dsl;
  private final SqlInterface sqlInterface;
  private final BackendConnectionImpl backendConnection;

  public BackendTransactionImpl(Connection connection, SqlInterface sqlInterface,
      BackendConnectionImpl backendConnection) {
    this.connection = connection;
    this.dsl = sqlInterface.getDslContextFactory().createDslContext(connection);
    this.sqlInterface = sqlInterface;
    this.backendConnection = backendConnection;
  }

  boolean isClosed() {
    return closed;
  }

  Connection getConnection() {
    return connection;
  }

  DSLContext getDsl() {
    return dsl;
  }

  SqlInterface getSqlInterface() {
    return sqlInterface;
  }

  BackendConnectionImpl getBackendConnection() {
    return backendConnection;
  }

  @Override
  public long getDatabaseSize(MetaDatabase db) {
    return sqlInterface.getMetaDataReadInterface().getDatabaseSize(getDsl(), db);
  }

  @Override
  public long countAll(MetaDatabase db, MetaCollection col) {
    return sqlInterface.getReadInterface().countAll(getDsl(), db, col);
  }

  @Override
  public long getCollectionSize(MetaDatabase db, MetaCollection col) {
    return sqlInterface.getMetaDataReadInterface().getCollectionSize(getDsl(), db, col);
  }

  @Override
  public long getDocumentsSize(MetaDatabase db, MetaCollection col) {
    return sqlInterface.getMetaDataReadInterface().getDocumentsSize(getDsl(), db, col);
  }

  @Override
  public BackendCursor findAll(MetaDatabase db, MetaCollection col) {
    try {
      Cursor<Integer> allDids = sqlInterface.getReadInterface().getAllCollectionDids(dsl, db, col);
      return new LazyBackendCursor(sqlInterface, allDids, dsl, db, col);
    } catch (SQLException ex) {
      throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
    }
  }

  @Override
  public BackendCursor findByField(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      MetaField field, KvValue<?> value) {
    try {
      Cursor<Integer> allDids = sqlInterface.getReadInterface().getCollectionDidsWithFieldEqualsTo(
          dsl, db, col, docPart, field, value);
      return new LazyBackendCursor(sqlInterface, allDids, dsl, db, col);
    } catch (SQLException ex) {
      throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
    }
  }

  @Override
  public BackendCursor findByFieldIn(MetaDatabase db, MetaCollection col, MetaDocPart docPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap) {
    try {
      if (valuesMultimap.isEmpty()) {
        return new EmptyBackendCursor();
      }
      Cursor<Integer> allDids = sqlInterface.getReadInterface().getCollectionDidsWithFieldsIn(dsl,
          db, col, docPart, valuesMultimap);
      return new LazyBackendCursor(sqlInterface, allDids, dsl, db, col);
    } catch (SQLException ex) {
      throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
    }
  }

  @Override
  public Cursor<Tuple2<Integer, KvValue<?>>> findByFieldInProjection(MetaDatabase db,
      MetaCollection col, MetaDocPart docPart,
      Multimap<MetaField, KvValue<?>> valuesMultimap) {
    try {
      if (valuesMultimap.isEmpty()) {
        return new EmptyCursor<>();
      }
      return sqlInterface.getReadInterface()
          .getCollectionDidsAndProjectionWithFieldsIn(dsl, db, col, docPart, valuesMultimap);
    } catch (SQLException ex) {
      throw sqlInterface.getErrorHandler().handleException(Context.FETCH, ex);
    }
  }

  @Override
  public BackendCursor fetch(MetaDatabase db, MetaCollection col, Cursor<Integer> didCursor) {
    return new LazyBackendCursor(sqlInterface, didCursor, dsl, db, col);
  }

  @Override
  public Optional<KvValue<?>> readMetaInfo(MetaInfoKey key) throws
      IllegalArgumentException {
    return getBackendConnection().getMetaInfoHandler().readMetaInfo(getDsl(), key);
  }

  @Override
  public void checkMetaDataTables() throws InvalidDatabaseException {
    getSqlInterface().getStructureInterface().checkMetaDataTables(getDsl());
  }

  @Override
  public void rollback() {
    try {
      connection.rollback();
    } catch (SQLException ex) {
      sqlInterface.getErrorHandler().handleException(Context.ROLLBACK, ex);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      backendConnection.onTransactionClosed(this);
      try {
        connection.rollback();
        connection.close();
      } catch (SQLException ex) {
        sqlInterface.getErrorHandler().handleException(Context.CLOSE, ex);
      } finally {
        dsl.close();
      }
    }
  }

}
