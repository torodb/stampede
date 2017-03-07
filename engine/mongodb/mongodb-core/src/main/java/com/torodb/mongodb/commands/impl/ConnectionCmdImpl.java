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

package com.torodb.mongodb.commands.impl;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.google.common.collect.ImmutableMap;
import com.torodb.core.BuildProperties;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.core.logging.LoggerFactory;
import com.torodb.mongodb.commands.CmdImplMapSupplier;
import com.torodb.mongodb.commands.impl.authentication.GetNonceImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.BuildInfoImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.GetLogImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.PingImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ServerStatusImplementation;
import com.torodb.mongodb.commands.impl.replication.IsMasterImplementation;
import com.torodb.mongodb.commands.signatures.authentication.GetNonceCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.BuildInfoCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.GetLogCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.PingCommand;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand;
import com.torodb.mongodb.commands.signatures.repl.IsMasterCommand;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;

import java.time.Clock;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
@SuppressWarnings("checkstyle:LineLength")
public class ConnectionCmdImpl implements CmdImplMapSupplier<MongodConnection> {

  private final ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>> map;

  @Inject
  ConnectionCmdImpl(LoggerFactory loggerFactory, Clock clock, BuildProperties buildProp,
      MongodServerConfig mongodServerConfig) {
    map = ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>>builder()
        .put(GetNonceCommand.INSTANCE, new GetNonceImplementation(loggerFactory))
        .put(BuildInfoCommand.INSTANCE, new BuildInfoImplementation(buildProp))
        .put(ServerStatusCommand.INSTANCE, new ServerStatusImplementation(
            mongodServerConfig,loggerFactory))
        .put(GetLogCommand.INSTANCE, new GetLogImplementation())
        .put(PingCommand.INSTANCE, new PingImplementation())
        .put(IsMasterCommand.INSTANCE, new IsMasterImplementation(
            clock, mongodServerConfig))
        .build();
  }

  @DoNotChange
  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  @Override
  public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>> get() {
    return map;
  }
}
