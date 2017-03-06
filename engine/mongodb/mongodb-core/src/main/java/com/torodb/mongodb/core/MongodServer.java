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

package com.torodb.mongodb.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.torodb.core.annotations.TorodbIdleService;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.core.services.IdleTorodbService;
import com.torodb.mongodb.commands.CommandClassifier;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.torod.TorodServer;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class MongodServer extends IdleTorodbService {

  private final LoggerFactory loggerFactory;
  private final Logger logger;
  private final TorodServer torodServer;
  private final Cache<Integer, MongodConnection> openConnections;
  private final CommandClassifier commandsExecutorClassifier;
  private final MongodMetrics metrics;
  private final ObjectIdFactory objectIdFactory;

  @Inject
  public MongodServer(@TorodbIdleService ThreadFactory threadFactory,
      LoggerFactory loggerFactory,
      TorodServer torodServer,
      CommandClassifier commandsExecutorClassifier,
      MongodMetrics metrics,
      ObjectIdFactory objectIdFactory) {
    super(threadFactory);
    this.loggerFactory = loggerFactory;
    this.logger = loggerFactory.apply(this.getClass());
    this.torodServer = torodServer;
    openConnections = CacheBuilder.newBuilder()
        .weakValues()
        .removalListener(this::onConnectionInvalidated)
        .build();
    this.commandsExecutorClassifier = commandsExecutorClassifier;
    this.metrics = metrics;
    this.objectIdFactory = objectIdFactory;
  }

  public TorodServer getTorodServer() {
    return torodServer;
  }

  public MongodConnection openConnection() {
    MongodConnection connection = new MongodConnection(this);
    openConnections.put(connection.getConnectionId(), connection);

    return connection;
  }

  public MongodMetrics getMetrics() {
    return metrics;
  }

  public ObjectIdFactory getObjectIdFactory() {
    return objectIdFactory;
  }

  @Override
  protected void startUp() throws Exception {
    logger.debug("Waiting for Torod server to be running");
    torodServer.awaitRunning();
    logger.debug("MongodServer ready to run");
  }

  @Override
  protected void shutDown() throws Exception {
    openConnections.invalidateAll();
  }

  public CommandClassifier getCommandsExecutorClassifier() {
    return commandsExecutorClassifier;
  }

  void onConnectionClose(MongodConnection connection) {
    openConnections.invalidate(connection.getConnectionId());
  }

  private void onConnectionInvalidated(
      RemovalNotification<Integer, MongodConnection> notification) {
    MongodConnection value = notification.getValue();
    if (value != null) {
      value.close();
    }
  }

  public LoggerFactory getLoggerFactory() {
    return loggerFactory;
  }

}
