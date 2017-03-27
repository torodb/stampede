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

package com.torodb.mongodb.repl.sharding.isolation.db;

import com.torodb.common.util.Empty;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;


public class DbIsolatorServer extends IdleTorodbService implements TorodServer {

  private final Logger logger;
  private final String shardId;
  private final TorodServer decorated;

  public DbIsolatorServer(String shardId, TorodServer decorated, ThreadFactory threadFactory,
      LoggerFactory lf) {
    super(threadFactory);
    this.logger = lf.apply(this.getClass());
    assert decorated.isRunning() : "The decorated torod server must be running";
    this.decorated = decorated;
    this.shardId = shardId;
  }

  @Override
  public TorodConnection openConnection() {
    return new DbIsolatorConn(this, decorated.openConnection());
  }

  final String convertDatabaseName(String dbName) {
    return dbName + "_" + shardId;
  }

  final String convertIndexName(String indexName) {
    return indexName + "_" + shardId;
  }

  final boolean isVisibleDatabase(String dbName) {
    return dbName.endsWith("_" + dbName);
  }

  @Override
  public CompletableFuture<Empty> disableDataImportMode(String dbName) {
    return decorated.disableDataImportMode(convertDatabaseName(dbName));
  }

  @Override
  public CompletableFuture<Empty> enableDataImportMode(String dbName) {
    return decorated.enableDataImportMode(convertDatabaseName(dbName));
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

}
