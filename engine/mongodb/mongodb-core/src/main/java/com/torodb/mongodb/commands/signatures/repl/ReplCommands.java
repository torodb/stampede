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

package com.torodb.mongodb.commands.signatures.repl;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsArgument;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsReply;
import com.torodb.mongodb.commands.signatures.repl.IsMasterCommand.IsMasterReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetReconfigCommand.ReplSetReconfigArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 */
public class ReplCommands implements Iterable<Command> {

  private final ImmutableList<Command> commands = ImmutableList.<Command>of(
      ApplyOpsCommand.INSTANCE,
      IsMasterCommand.INSTANCE,
      ReplSetFreezeCommand.INSTANCE,
      ReplSetGetConfigCommand.INSTANCE,
      ReplSetGetStatusCommand.INSTANCE,
      ReplSetInitiateCommand.INSTANCE,
      ReplSetMaintenanceCommand.INSTANCE,
      ReplSetReconfigCommand.INSTANCE,
      ReplSetStepDownCommand.INSTANCE,
      ReplSetSyncFromCommand.INSTANCE
  );

  @Override
  public Iterator<Command> iterator() {
    return commands.iterator();
  }

  @SuppressWarnings("checkstyle:LineLength")
  public abstract static class ReplCommandsImplementationsBuilder<ContextT> implements
      Iterable<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> {

    public abstract CommandImplementation<ApplyOpsArgument, ApplyOpsReply, ? super ContextT> getApplyOpsImplementation();

    public abstract CommandImplementation<Empty, IsMasterReply, ? super ContextT> getIsMasterImplementation();

    public abstract CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, ? super ContextT> getReplSetFreezeImplementation();

    public abstract CommandImplementation<Empty, ReplicaSetConfig, ? super ContextT> getReplSetGetConfigImplementation();

    public abstract CommandImplementation<Empty, ReplSetGetStatusReply, ? super ContextT> getReplSetGetStatusImplementation();

    public abstract CommandImplementation<ReplicaSetConfig, Empty, ? super ContextT> getReplSetInitiateImplementation();

    public abstract CommandImplementation<Boolean, Empty, ? super ContextT> getReplSetMaintenanceImplementation();

    public abstract CommandImplementation<ReplSetReconfigArgument, Empty, ? super ContextT> getReplSetReconfigImplementation();

    public abstract CommandImplementation<ReplSetStepDownArgument, Empty, ? super ContextT> getReplSetStepDownImplementation();

    public abstract CommandImplementation<HostAndPort, ReplSetSyncFromReply, ? super ContextT> getReplSetSyncFromImplementation();

    private Map<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>> createMap() {
      return ImmutableMap.<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>builder()
          .put(ApplyOpsCommand.INSTANCE, getApplyOpsImplementation())
          .put(IsMasterCommand.INSTANCE, getIsMasterImplementation())
          .put(ReplSetFreezeCommand.INSTANCE, getReplSetFreezeImplementation())
          .put(ReplSetGetConfigCommand.INSTANCE, getReplSetGetConfigImplementation())
          .put(ReplSetGetStatusCommand.INSTANCE, getReplSetGetStatusImplementation())
          .put(ReplSetInitiateCommand.INSTANCE, getReplSetInitiateImplementation())
          .put(ReplSetMaintenanceCommand.INSTANCE, getReplSetMaintenanceImplementation())
          .put(ReplSetReconfigCommand.INSTANCE, getReplSetReconfigImplementation())
          .put(ReplSetStepDownCommand.INSTANCE, getReplSetStepDownImplementation())
          .put(ReplSetSyncFromCommand.INSTANCE, getReplSetSyncFromImplementation())
          .build();
    }

    @Override
    public Iterator<Entry<Command<?, ?>, CommandImplementation<?, ?, ? super ContextT>>> iterator() {
      return createMap().entrySet().iterator();
    }

  }

}
