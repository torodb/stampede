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

package com.torodb.mongodb.repl.commands;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.mongodb.commands.AbstractCommandMapFactory;
import com.torodb.mongodb.commands.ExclusiveWriteTransactionImplementations;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
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
import com.torodb.mongodb.core.ExclusiveWriteMongodTransaction;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.commands.impl.ExclusiveReplCommandImpl;
import com.torodb.mongodb.repl.commands.impl.RenameCollectionReplImpl;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@SuppressWarnings("checkstyle:LineLength")
public class ReplExclusiveWriteTransactionImplementations extends ExclusiveWriteTransactionImplementations {

  @Inject
  protected ReplExclusiveWriteTransactionImplementations(Injector injector) {
    super(new MapFactory(injector));
  }

  protected static class MapFactory extends AbstractCommandMapFactory<ExclusiveWriteMongodTransaction> {

    @Inject
    public MapFactory(Injector injector) {
      super(new MyAdminCommandsImplementationBuilder(injector),
          new MyAggregationCommandsImplementationBuilder(),
          new MyAuthenticationCommandsImplementationsBuilder(),
          new MyDiagnosticCommandsImplementationBuilder(),
          new MyGeneralCommandsImplementationBuilder(),
          new MyInternalCommandsImplementationsBuilder(),
          new MyReplCommandsImplementationsBuilder());
    }

  }

  static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    private final ReplicationFilters replicationFilters;

    public MyAdminCommandsImplementationBuilder(Injector injector) {
      replicationFilters = injector.getInstance(ReplicationFilters.class);
    }

    @Override
    public CommandImplementation<CollModArgument, CollModResult, ? super ExclusiveWriteMongodTransaction> getCollModImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, ExclusiveWriteMongodTransaction> getListCollectionsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, ExclusiveWriteMongodTransaction> getDropDatabaseImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CollectionCommandArgument, Empty, ExclusiveWriteMongodTransaction> getDropCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateCollectionArgument, Empty, ExclusiveWriteMongodTransaction> getCreateCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListIndexesArgument, ListIndexesResult, ExclusiveWriteMongodTransaction> getListIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, ExclusiveWriteMongodTransaction> getCreateIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DropIndexesArgument, DropIndexesResult, ? super ExclusiveWriteMongodTransaction> getDropIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<RenameCollectionArgument, Empty, ? super ExclusiveWriteMongodTransaction> getRenameCollectionImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new RenameCollectionReplImpl(
          replicationFilters));
    }
  }

  static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<CountArgument, Long, ExclusiveWriteMongodTransaction> getCountImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<Empty, String, ExclusiveWriteMongodTransaction> getGetNonceImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<CollStatsArgument, CollStatsReply, ExclusiveWriteMongodTransaction> getCollStatsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ListDatabasesReply, ExclusiveWriteMongodTransaction> getListDatabasesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, BuildInfoResult, ExclusiveWriteMongodTransaction> getBuildInfoImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ServerStatusArgument, ServerStatusReply, ExclusiveWriteMongodTransaction> getServerStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<GetLogArgument, GetLogReply, ExclusiveWriteMongodTransaction> getGetLogImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, ExclusiveWriteMongodTransaction> getPingCommandImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, ExclusiveWriteMongodTransaction> getGetLastErrrorImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<InsertArgument, InsertResult, ExclusiveWriteMongodTransaction> getInsertImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<FindArgument, FindResult, ? super ExclusiveWriteMongodTransaction> getFindImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DeleteArgument, Long, ExclusiveWriteMongodTransaction> getDeleteImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<UpdateArgument, UpdateResult, ExclusiveWriteMongodTransaction> getUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<HandshakeArgument, Empty, ExclusiveWriteMongodTransaction> getHandshakeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetRBIDReply, ExclusiveWriteMongodTransaction> getReplSetGetRBIDImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetUpdatePositionArgument, Empty, ExclusiveWriteMongodTransaction> getReplSetUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetElectArgument, ReplSetElectReply, ExclusiveWriteMongodTransaction> getReplSetElectImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, ExclusiveWriteMongodTransaction> getReplSetFreshImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, ExclusiveWriteMongodTransaction> getReplSetHeartbeatImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, WhatsMyUriReply, ExclusiveWriteMongodTransaction> getWhatsMyUriImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<ExclusiveWriteMongodTransaction> {

    @Override
    public CommandImplementation<ApplyOpsArgument, ApplyOpsReply, ExclusiveWriteMongodTransaction> getApplyOpsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, ExclusiveWriteMongodTransaction> getReplSetFreezeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, IsMasterReply, ExclusiveWriteMongodTransaction> getIsMasterImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplicaSetConfig, ExclusiveWriteMongodTransaction> getReplSetGetConfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetStatusReply, ExclusiveWriteMongodTransaction> getReplSetGetStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplicaSetConfig, Empty, ExclusiveWriteMongodTransaction> getReplSetInitiateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Boolean, Empty, ExclusiveWriteMongodTransaction> getReplSetMaintenanceImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetReconfigArgument, Empty, ExclusiveWriteMongodTransaction> getReplSetReconfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetStepDownArgument, Empty, ExclusiveWriteMongodTransaction> getReplSetStepDownImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<HostAndPort, ReplSetSyncFromReply, ExclusiveWriteMongodTransaction> getReplSetSyncFromImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  private static class ReplImplToCommandImplementationAdapter<A, R> implements
      CommandImplementation<A, R, ExclusiveWriteMongodTransaction> {

    private final ExclusiveReplCommandImpl<A, R> replCommand;

    public ReplImplToCommandImplementationAdapter(ExclusiveReplCommandImpl<A, R> replCommand) {
      super();
      this.replCommand = replCommand;
    }

    @Override
    public Status<R> apply(Request req, Command<? super A, ? super R> command, A arg,
        ExclusiveWriteMongodTransaction context) {
      return replCommand.apply(req, command, arg, context.getTorodTransaction());
    }
  }
}
