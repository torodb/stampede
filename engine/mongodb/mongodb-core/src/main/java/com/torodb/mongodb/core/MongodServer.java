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
import com.torodb.core.services.IdleTorodbService;
import com.torodb.mongodb.commands.CommandsExecutorClassifier;
import com.torodb.torod.TorodServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class MongodServer extends IdleTorodbService {

  private static final Logger LOGGER = LogManager.getLogger(MongodServer.class);
  private final TorodServer torodServer;
  private final Cache<Integer, MongodConnection> openConnections;
  private final CommandsExecutorClassifier commandsExecutorClassifier;

  @Inject
  public MongodServer(@TorodbIdleService ThreadFactory threadFactory,
      TorodServer torodServer,
      CommandsExecutorClassifier commandsExecutorClassifier) {
    super(threadFactory);
    this.torodServer = torodServer;
    openConnections = CacheBuilder.newBuilder()
        .weakValues()
        .removalListener(this::onConnectionInvalidated)
        .build();
    this.commandsExecutorClassifier = commandsExecutorClassifier;
  }

  public TorodServer getTorodServer() {
    return torodServer;
  }

  public MongodConnection openConnection() {
    MongodConnection connection = new MongodConnection(this);
    openConnections.put(connection.getConnectionId(), connection);

    return connection;
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.debug("Waiting for Torod server to be running");
    torodServer.awaitRunning();
    LOGGER.debug("MongodServer ready to run");
  }

  @Override
  protected void shutDown() throws Exception {
    openConnections.invalidateAll();
  }

  public CommandsExecutorClassifier getCommandsExecutorClassifier() {
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

}
