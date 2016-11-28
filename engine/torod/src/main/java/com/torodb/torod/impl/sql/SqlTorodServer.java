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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.torodb.core.TableRefFactory;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.backend.BackendService;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.core.transaction.InternalTransactionManager;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.torod.TorodServer;
import com.torodb.torod.pipeline.InsertPipelineFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

/**
 *
 */
public class SqlTorodServer extends IdleTorodbService implements TorodServer {

  private final AtomicInteger connectionIdCounter = new AtomicInteger();
  private final D2RTranslatorFactory d2RTranslatorFactory;
  private final R2DTranslator r2DTranslator;
  private final IdentifierFactory idFactory;
  private final InsertPipelineFactory insertPipelineFactory;
  private final Cache<Integer, SqlTorodConnection> openConnections;
  private final BackendService backend;
  private final InternalTransactionManager internalTransactionManager;
  private final TableRefFactory tableRefFactory;

  @Inject
  public SqlTorodServer(@TorodbIdleService ThreadFactory threadFactory,
      D2RTranslatorFactory d2RTranslatorFactory,
      R2DTranslator r2DTranslator, IdentifierFactory idFactory,
      InsertPipelineFactory insertPipelineFactory,
      BackendService backend, TableRefFactory tableRefFactory,
      InternalTransactionManager internalTransactionManager) {
    super(threadFactory);
    this.d2RTranslatorFactory = d2RTranslatorFactory;
    this.r2DTranslator = r2DTranslator;
    this.idFactory = idFactory;
    this.insertPipelineFactory = insertPipelineFactory;
    this.backend = backend;
    this.internalTransactionManager = internalTransactionManager;
    this.tableRefFactory = tableRefFactory;

    openConnections = CacheBuilder.newBuilder()
        .weakValues()
        .removalListener(this::onConnectionInvalidated)
        .build();
  }

  @Override
  public SqlTorodConnection openConnection() {
    int connectionId = connectionIdCounter.incrementAndGet();
    SqlTorodConnection connection = new SqlTorodConnection(this, connectionId);
    openConnections.put(connectionId, connection);

    return connection;
  }

  @Override
  protected void startUp() throws Exception {
    backend.awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    openConnections.invalidateAll();
  }

  private void onConnectionInvalidated(
      RemovalNotification<Integer, SqlTorodConnection> notification) {
    SqlTorodConnection value = notification.getValue();
    if (value != null) {
      value.close();
    }
  }

  @Override
  public void enableDataImportMode() {
    ImmutableMetaSnapshot snapshot = internalTransactionManager.takeMetaSnapshot();
    backend.enableDataImportMode(snapshot);
  }

  @Override
  public void disableDataImportMode() {
    ImmutableMetaSnapshot snapshot = internalTransactionManager.takeMetaSnapshot();
    backend.disableDataImportMode(snapshot);
  }

  D2RTranslatorFactory getD2RTranslatorFactory() {
    return d2RTranslatorFactory;
  }

  IdentifierFactory getIdentifierFactory() {
    return idFactory;
  }

  InsertPipelineFactory getInsertPipelineFactory() {
    return insertPipelineFactory;
  }

  BackendService getBackend() {
    return backend;
  }

  InternalTransactionManager getInternalTransactionManager() {
    return internalTransactionManager;
  }

  void onConnectionClosed(SqlTorodConnection connection) {
    openConnections.invalidate(connection.getConnectionId());
  }

  TableRefFactory getTableRefFactory() {
    return tableRefFactory;
  }

  R2DTranslator getR2DTranslator() {
    return r2DTranslator;
  }

}
