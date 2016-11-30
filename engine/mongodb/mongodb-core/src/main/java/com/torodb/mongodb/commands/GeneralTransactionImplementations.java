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
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.admin.ListCollectionsImplementation;
import com.torodb.mongodb.commands.impl.admin.ListIndexesImplementation;
import com.torodb.mongodb.commands.impl.aggregation.CountImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.CollStatsImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ListDatabasesImplementation;
import com.torodb.mongodb.commands.impl.general.FindImplementation;
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
import com.torodb.mongodb.core.MongodTransaction;

import java.util.Set;
import java.util.function.Supplier;

import javax.inject.Inject;

/**
 * This class contains the implementations of all commands that can be executed on a read or write
 * transaction.
 */
@SuppressWarnings("checkstyle:LineLength")
public class GeneralTransactionImplementations {

  private final ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> map;

  @Inject
  GeneralTransactionImplementations(Injector injector) {
    this(new MapFactory(injector));
  }

  protected GeneralTransactionImplementations(
      Supplier<ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>>> mapFactory) {
    map = mapFactory.get();
  }

  Set<Command<?, ?>> getSupportedCommands() {
    return map.keySet();
  }

  public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> getMap() {
    return map;
  }

  static class MapFactory extends AbstractCommandMapFactory<MongodTransaction> {

    @Inject
    public MapFactory(Injector injector) {
      super(
          new MyAdminCommandsImplementationBuilder(),
          new MyAggregationCommandsImplementationBuilder(),
          new MyAuthenticationCommandsImplementationsBuilder(injector),
          new MyDiagnosticCommandsImplementationBuilder(),
          new MyGeneralCommandsImplementationBuilder(),
          new MyInternalCommandsImplementationsBuilder(),
          new MyReplCommandsImplementationsBuilder()
      );
    }

  }

  static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<CollModArgument, CollModResult, ? super MongodTransaction> getCollModImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, ? super MongodTransaction> getListCollectionsImplementation() {
      return new ListCollectionsImplementation();
    }

    @Override
    public CommandImplementation<Empty, Empty, MongodTransaction> getDropDatabaseImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CollectionCommandArgument, Empty, MongodTransaction> getDropCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateCollectionArgument, Empty, MongodTransaction> getCreateCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListIndexesArgument, ListIndexesResult, MongodTransaction> getListIndexesImplementation() {
      return new ListIndexesImplementation();
    }

    @Override
    public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, MongodTransaction> getCreateIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DropIndexesArgument, DropIndexesResult, ? super MongodTransaction> getDropIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<RenameCollectionArgument, Empty, ? super MongodTransaction> getRenameCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<CountArgument, Long, ? super MongodTransaction> getCountImplementation() {
      return new CountImplementation();
    }

  }

  static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<MongodTransaction> {

    private MyAuthenticationCommandsImplementationsBuilder(Injector injector) {
    }

    @Override
    public CommandImplementation<Empty, String, MongodTransaction> getGetNonceImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<CollStatsArgument, CollStatsReply, ? super MongodTransaction> getCollStatsImplementation() {
      return new CollStatsImplementation();
    }

    @Override
    public CommandImplementation<Empty, ListDatabasesReply, ? super MongodTransaction> getListDatabasesImplementation() {
      return new ListDatabasesImplementation();
    }

    @Override
    public CommandImplementation<Empty, BuildInfoResult, MongodTransaction> getBuildInfoImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ServerStatusArgument, ServerStatusReply, MongodTransaction> getServerStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<GetLogArgument, GetLogReply, MongodTransaction> getGetLogImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, MongodTransaction> getPingCommandImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, MongodTransaction> getGetLastErrrorImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<FindArgument, FindResult, ? super MongodTransaction> getFindImplementation() {
      return new FindImplementation();
    }

    @Override
    public CommandImplementation<InsertArgument, InsertResult, MongodTransaction> getInsertImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DeleteArgument, Long, MongodTransaction> getDeleteImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<UpdateArgument, UpdateResult, MongodTransaction> getUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<HandshakeArgument, Empty, MongodTransaction> getHandshakeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetRBIDReply, MongodTransaction> getReplSetGetRBIDImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetUpdatePositionArgument, Empty, MongodTransaction> getReplSetUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetElectArgument, ReplSetElectReply, MongodTransaction> getReplSetElectImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, MongodTransaction> getReplSetFreshImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, MongodTransaction> getReplSetHeartbeatImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, WhatsMyUriReply, MongodTransaction> getWhatsMyUriImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<MongodTransaction> {

    @Override
    public CommandImplementation<ApplyOpsArgument, ApplyOpsReply, MongodTransaction> getApplyOpsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, MongodTransaction> getReplSetFreezeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, IsMasterReply, MongodTransaction> getIsMasterImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplicaSetConfig, MongodTransaction> getReplSetGetConfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetStatusReply, MongodTransaction> getReplSetGetStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplicaSetConfig, Empty, MongodTransaction> getReplSetInitiateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Boolean, Empty, MongodTransaction> getReplSetMaintenanceImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetReconfigArgument, Empty, MongodTransaction> getReplSetReconfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetStepDownArgument, Empty, MongodTransaction> getReplSetStepDownImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<HostAndPort, ReplSetSyncFromReply, MongodTransaction> getReplSetSyncFromImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }
}
