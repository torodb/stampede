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

package com.torodb.mongodb.commands;

import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.BuildProperties;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.authentication.GetNonceImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.BuildInfoImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.GetLogImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.PingImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ServerStatusImplementation;
import com.torodb.mongodb.commands.impl.internal.WhatsMyUriImplementation;
import com.torodb.mongodb.commands.impl.replication.IsMasterImplementation;
import com.torodb.mongodb.commands.pojos.ReplicaSetConfig;
import com.torodb.mongodb.commands.signatures.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.admin.CollModCommand.CollModArgument;
import com.torodb.mongodb.commands.signatures.admin.CollModCommand.CollModResult;
import com.torodb.mongodb.commands.signatures.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.CreateIndexesCommand.CreateIndexesResult;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.DropIndexesCommand.DropIndexesResult;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.torodb.mongodb.commands.signatures.admin.ListCollectionsCommand.ListCollectionsResult;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesArgument;
import com.torodb.mongodb.commands.signatures.admin.ListIndexesCommand.ListIndexesResult;
import com.torodb.mongodb.commands.signatures.admin.RenameCollectionCommand.RenameCollectionArgument;
import com.torodb.mongodb.commands.signatures.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.aggregation.CountCommand.CountArgument;
import com.torodb.mongodb.commands.signatures.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.diagnostic.BuildInfoCommand.BuildInfoResult;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.CollStatsCommand.CollStatsReply;
import com.torodb.mongodb.commands.signatures.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.diagnostic.GetLogCommand.GetLogArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.GetLogCommand.GetLogReply;
import com.torodb.mongodb.commands.signatures.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.torodb.mongodb.commands.signatures.diagnostic.ServerStatusCommand.ServerStatusReply;
import com.torodb.mongodb.commands.signatures.general.DeleteCommand.DeleteArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindArgument;
import com.torodb.mongodb.commands.signatures.general.FindCommand.FindResult;
import com.torodb.mongodb.commands.signatures.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorArgument;
import com.torodb.mongodb.commands.signatures.general.GetLastErrorCommand.GetLastErrorReply;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertArgument;
import com.torodb.mongodb.commands.signatures.general.InsertCommand.InsertResult;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateArgument;
import com.torodb.mongodb.commands.signatures.general.UpdateCommand.UpdateResult;
import com.torodb.mongodb.commands.signatures.internal.HandshakeCommand.HandshakeArgument;
import com.torodb.mongodb.commands.signatures.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.internal.ReplSetElectCommand.ReplSetElectArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetElectCommand.ReplSetElectReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetFreshCommand.ReplSetFreshReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetGetRBIDCommand.ReplSetGetRBIDReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.torodb.mongodb.commands.signatures.internal.ReplSetHeartbeatReply;
import com.torodb.mongodb.commands.signatures.internal.ReplSetUpdatePositionCommand.ReplSetUpdatePositionArgument;
import com.torodb.mongodb.commands.signatures.internal.WhatsMyUriCommand.WhatsMyUriReply;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsArgument;
import com.torodb.mongodb.commands.signatures.repl.ApplyOpsCommand.ApplyOpsReply;
import com.torodb.mongodb.commands.signatures.repl.IsMasterCommand.IsMasterReply;
import com.torodb.mongodb.commands.signatures.repl.ReplCommands.ReplCommandsImplementationsBuilder;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetFreezeCommand.ReplSetFreezeReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.torodb.mongodb.commands.signatures.repl.ReplSetReconfigCommand.ReplSetReconfigArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;
import com.torodb.mongodb.commands.signatures.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.MongodServerConfig;

import java.time.Clock;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

@ThreadSafe
@SuppressWarnings("checkstyle:LineLength")
public class ConnectionCommandsExecutor {

  private final ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>> map;

  @Inject
  ConnectionCommandsExecutor(Injector injector) {
    this(new MapFactory(injector));
  }

  protected ConnectionCommandsExecutor(
      Supplier<ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>>> mapFactory) {
    map = mapFactory.get();
  }

  @DoNotChange
  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>> getMap() {
    return map;
  }

  static class MapFactory extends AbstractCommandMapFactory<MongodConnection> {

    public MapFactory(Injector injector) {
      super(
          new MyAdminCommandsImplementationBuilder(),
          new MyAggregationCommandsImplementationBuilder(),
          new MyAuthenticationCommandsImplementationsBuilder(injector),
          new MyDiagnosticCommandsImplementationBuilder(injector),
          new MyGeneralCommandsImplementationBuilder(),
          new MyInternalCommandsImplementationsBuilder(),
          new MyReplCommandsImplementationsBuilder(injector)
      );
    }

  }

  static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<MongodConnection> {

    @Override
    public CommandImplementation<CollModArgument, CollModResult, ? super MongodConnection> getCollModImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, MongodConnection> getListCollectionsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, MongodConnection> getDropDatabaseImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CollectionCommandArgument, Empty, MongodConnection> getDropCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateCollectionArgument, Empty, MongodConnection> getCreateCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListIndexesArgument, ListIndexesResult, MongodConnection> getListIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, MongodConnection> getCreateIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DropIndexesArgument, DropIndexesResult, ? super MongodConnection> getDropIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<RenameCollectionArgument, Empty, ? super MongodConnection> getRenameCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<MongodConnection> {

    @Override
    public CommandImplementation<CountArgument, Long, MongodConnection> getCountImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<MongodConnection> {

    private MyAuthenticationCommandsImplementationsBuilder(Injector injector) {
    }

    @Override
    public GetNonceImplementation getGetNonceImplementation() {
      return new GetNonceImplementation();
    }

  }

  static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<MongodConnection> {

    private final Injector injector;

    public MyDiagnosticCommandsImplementationBuilder(Injector injector) {
      this.injector = injector;
    }

    @Override
    public CommandImplementation<CollStatsArgument, CollStatsReply, MongodConnection> getCollStatsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ListDatabasesReply, MongodConnection> getListDatabasesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, BuildInfoResult, MongodConnection> getBuildInfoImplementation() {
      return new BuildInfoImplementation(injector.getInstance(BuildProperties.class));
    }

    @Override
    public CommandImplementation<ServerStatusArgument, ServerStatusReply, MongodConnection> getServerStatusImplementation() {
      return new ServerStatusImplementation(injector.getInstance(MongodServerConfig.class));
    }

    @Override
    public CommandImplementation<GetLogArgument, GetLogReply, MongodConnection> getGetLogImplementation() {
      return new GetLogImplementation();
    }

    @Override
    public CommandImplementation<Empty, Empty, MongodConnection> getPingCommandImplementation() {
      return new PingImplementation();
    }

  }

  static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder<MongodConnection> {

    @Override
    public CommandImplementation<FindArgument, FindResult, MongodConnection> getFindImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, MongodConnection> getGetLastErrrorImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<InsertArgument, InsertResult, MongodConnection> getInsertImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DeleteArgument, Long, MongodConnection> getDeleteImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<UpdateArgument, UpdateResult, MongodConnection> getUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder<MongodConnection> {

    @Override
    public CommandImplementation<HandshakeArgument, Empty, MongodConnection> getHandshakeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetRBIDReply, MongodConnection> getReplSetGetRBIDImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetUpdatePositionArgument, Empty, MongodConnection> getReplSetUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetElectArgument, ReplSetElectReply, MongodConnection> getReplSetElectImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, MongodConnection> getReplSetFreshImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, MongodConnection> getReplSetHeartbeatImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, WhatsMyUriReply, MongodConnection> getWhatsMyUriImplementation() {
      return new WhatsMyUriImplementation();
    }
  }

  static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<MongodConnection> {

    private final Injector injector;

    public MyReplCommandsImplementationsBuilder(Injector injector) {
      this.injector = injector;
    }

    @Override
    public CommandImplementation<ApplyOpsArgument, ApplyOpsReply, MongodConnection> getApplyOpsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, MongodConnection> getReplSetFreezeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, IsMasterReply, MongodConnection> getIsMasterImplementation() {
      return new IsMasterImplementation(injector.getInstance(Clock.class), injector.getInstance(
          MongodServerConfig.class));
    }

    @Override
    public CommandImplementation<Empty, ReplicaSetConfig, MongodConnection> getReplSetGetConfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetStatusReply, MongodConnection> getReplSetGetStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplicaSetConfig, Empty, MongodConnection> getReplSetInitiateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Boolean, Empty, MongodConnection> getReplSetMaintenanceImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetReconfigArgument, Empty, MongodConnection> getReplSetReconfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetStepDownArgument, Empty, MongodConnection> getReplSetStepDownImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<HostAndPort, ReplSetSyncFromReply, MongodConnection> getReplSetSyncFromImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }
}
