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

package com.torodb.mongodb.commands.signatures.diagnostic;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.torodb.mongodb.commands.signatures.diagnostic.BuildInfoCommand.BuildInfoResult;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsReply;
import com.torodb.mongodb.commands.signatures.diagnostic.GetLogCommand.GetLogArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.GetLogCommand.GetLogReply;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusReply;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class DiagnosticCommands implements Iterable<Command> {

  private final ImmutableList<Command> commands = ImmutableList.<Command>of(
      CollStatsCommand.INSTANCE,
      ListDatabasesCommand.INSTANCE,
      BuildInfoCommand.INSTANCE,
      PingCommand.INSTANCE,
      ServerStatusCommand.INSTANCE,
      GetLogCommand.INSTANCE
  );

  @Override
  public Iterator<Command> iterator() {
    return commands.iterator();
  }

  @SuppressWarnings("checkstyle:LineLength")
  public abstract static class DiagnosticCommandsImplementationsBuilder<ContextT> implements
      Iterable<Map.Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    public abstract CommandImplementation<CollStatsArgument, CollStatsReply, ? super ContextT> getCollStatsImplementation();

    public abstract CommandImplementation<GetLogArgument, GetLogReply, ? super ContextT> getGetLogImplementation();

    public abstract CommandImplementation<Empty, ListDatabasesReply, ? super ContextT> getListDatabasesImplementation();

    public abstract CommandImplementation<Empty, Empty, ? super ContextT> getPingCommandImplementation();

    public abstract CommandImplementation<Empty, BuildInfoResult, ? super ContextT> getBuildInfoImplementation();

    public abstract CommandImplementation<ServerStatusArgument, ServerStatusReply, ? super ContextT> getServerStatusImplementation();

    private Map<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> createMap() {
      return ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>builder()
          .put(CollStatsCommand.INSTANCE, getCollStatsImplementation())
          .put(GetLogCommand.INSTANCE, getGetLogImplementation())
          .put(ListDatabasesCommand.INSTANCE, getListDatabasesImplementation())
          .put(BuildInfoCommand.INSTANCE, getBuildInfoImplementation())
          .put(PingCommand.INSTANCE, getPingCommandImplementation())
          .put(ServerStatusCommand.INSTANCE, getServerStatusImplementation())
          .build();
    }

    @Override
    public Iterator<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return createMap().entrySet().iterator();
    }

  }

}
