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

package com.torodb.mongodb.repl.topology;

import com.eightkdata.mongowp.client.core.MongoClient;
import com.eightkdata.mongowp.client.core.MongoClientFactory;
import com.eightkdata.mongowp.client.core.MongoConnection;
import com.eightkdata.mongowp.client.core.MongoConnection.RemoteCommandResponse;
import com.eightkdata.mongowp.client.core.UnreachableMongoServerException;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.core.concurrent.ConcurrentToolsFactory;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatCommand;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetGetConfigCommand;
import org.jooq.lambda.UncheckedException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

/**
 *
 */
public class MongoClientHeartbeatNetworkHandler implements HeartbeatNetworkHandler {

  private final MongoClientFactory mongoClientFactory;
  private final ExecutorService executorService;

  @Inject
  public MongoClientHeartbeatNetworkHandler(MongoClientFactory mongoClientFactory,
      ConcurrentToolsFactory concurrentToolsFactory) {
    this.mongoClientFactory = mongoClientFactory;
    executorService = concurrentToolsFactory.createExecutorService("topology-network", true);
  }

  @Override
  public CompletableFuture<RemoteCommandResponse<ReplSetHeartbeatReply>> sendHeartbeat(
      RemoteCommandRequest<ReplSetHeartbeatArgument> req) {
    return CompletableFuture.completedFuture(req)
        .thenApplyAsync(this::sendHeartbeatTask, executorService);
  }

  @Override
  public CompletableFuture<RemoteCommandResponse<ReplicaSetConfig>> askForConfig(
      RemoteCommandRequest<Empty> req) {
    return CompletableFuture.completedFuture(req)
        .thenApplyAsync(this::sendGetConfig, executorService);
  }

  private RemoteCommandResponse<ReplSetHeartbeatReply> sendHeartbeatTask(
      RemoteCommandRequest<ReplSetHeartbeatArgument> req) {
    try (MongoClient client = mongoClientFactory.createClient(req.getTarget());
        MongoConnection connection = client.openConnection()) {
      return connection.execute(
          ReplSetHeartbeatCommand.INSTANCE,
          req.getDbname(),
          true,
          req.getCmdObj()
      );
    } catch (UnreachableMongoServerException ex) {
      throw new UncheckedException(ex);
    }
  }

  private RemoteCommandResponse<ReplicaSetConfig> sendGetConfig(
      RemoteCommandRequest<Empty> req) {
    MongoClient client;
    try {
      client = mongoClientFactory.createClient(req.getTarget());
    } catch (UnreachableMongoServerException ex) {
      throw new UncheckedException(ex);
    }
    try (MongoConnection connection = client.openConnection()) {
      return connection.execute(
          ReplSetGetConfigCommand.INSTANCE,
          req.getDbname(),
          true,
          req.getCmdObj()
      );
    } finally {
      client.close();
    }
  }
}
