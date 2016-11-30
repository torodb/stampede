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
import com.torodb.mongodb.commands.WriteTransactionImplementations;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.general.DeleteImplementation;
import com.torodb.mongodb.commands.impl.general.InsertImplementation;
import com.torodb.mongodb.commands.impl.general.UpdateImplementation;
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
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.ObjectIdFactory;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.commands.impl.CreateCollectionReplImpl;
import com.torodb.mongodb.repl.commands.impl.CreateIndexesReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropCollectionReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropDatabaseReplImpl;
import com.torodb.mongodb.repl.commands.impl.DropIndexesReplImpl;
import com.torodb.mongodb.repl.commands.impl.ReplCommandImpl;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
@SuppressWarnings("checkstyle:LineLength")
public class ReplWriteTransactionImplementations extends WriteTransactionImplementations {

  @Inject
  protected ReplWriteTransactionImplementations(Injector injector) {
    super(new MapFactory(injector));
  }

  static class MapFactory extends AbstractCommandMapFactory<WriteMongodTransaction> {

    @Inject
    MapFactory(Injector injector) {
      super(new MyAdminCommandsImplementationBuilder(injector),
          new MyAggregationCommandsImplementationBuilder(),
          new MyAuthenticationCommandsImplementationsBuilder(),
          new MyDiagnosticCommandsImplementationBuilder(),
          new MyGeneralCommandsImplementationBuilder(injector),
          new MyInternalCommandsImplementationsBuilder(),
          new MyReplCommandsImplementationsBuilder());
    }

  }

  static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<WriteMongodTransaction> {

    private final ReplicationFilters replicationFilters;

    public MyAdminCommandsImplementationBuilder(Injector injector) {
      replicationFilters = injector.getInstance(ReplicationFilters.class);
    }

    @Override
    public CommandImplementation<CollModArgument, CollModResult, ? super WriteMongodTransaction> getCollModImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, WriteMongodTransaction> getListCollectionsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, WriteMongodTransaction> getDropDatabaseImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new DropDatabaseReplImpl());
    }

    @Override
    public CommandImplementation<CollectionCommandArgument, Empty, WriteMongodTransaction> getDropCollectionImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new DropCollectionReplImpl());
    }

    @Override
    public CommandImplementation<CreateCollectionArgument, Empty, WriteMongodTransaction> getCreateCollectionImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new CreateCollectionReplImpl());
    }

    @Override
    public CommandImplementation<ListIndexesArgument, ListIndexesResult, WriteMongodTransaction> getListIndexesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, WriteMongodTransaction> getCreateIndexesImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new CreateIndexesReplImpl(
          replicationFilters));
    }

    @Override
    public CommandImplementation<DropIndexesArgument, DropIndexesResult, ? super WriteMongodTransaction> getDropIndexesImplementation() {
      return new ReplImplToCommandImplementationAdapter<>(new DropIndexesReplImpl());
    }

    @Override
    public CommandImplementation<RenameCollectionArgument, Empty, ? super WriteMongodTransaction> getRenameCollectionImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<WriteMongodTransaction> {

    @Override
    public CommandImplementation<CountArgument, Long, WriteMongodTransaction> getCountImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<WriteMongodTransaction> {

    @Override
    public CommandImplementation<Empty, String, WriteMongodTransaction> getGetNonceImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<WriteMongodTransaction> {

    @Override
    public CommandImplementation<CollStatsArgument, CollStatsReply, WriteMongodTransaction> getCollStatsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ListDatabasesReply, WriteMongodTransaction> getListDatabasesImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, BuildInfoResult, WriteMongodTransaction> getBuildInfoImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ServerStatusArgument, ServerStatusReply, WriteMongodTransaction> getServerStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<GetLogArgument, GetLogReply, WriteMongodTransaction> getGetLogImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, Empty, WriteMongodTransaction> getPingCommandImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder<WriteMongodTransaction> {

    private final MongodMetrics mongodMetrics;
    private final ObjectIdFactory objectIdFactory;

    public MyGeneralCommandsImplementationBuilder(Injector injector) {
      this.mongodMetrics = injector.getInstance(MongodMetrics.class);
      this.objectIdFactory = injector.getInstance(ObjectIdFactory.class);
    }

    @Override
    public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, WriteMongodTransaction> getGetLastErrrorImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<InsertArgument, InsertResult, WriteMongodTransaction> getInsertImplementation() {
      return new InsertImplementation(mongodMetrics);
    }

    @Override
    public CommandImplementation<FindArgument, FindResult, ? super WriteMongodTransaction> getFindImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<DeleteArgument, Long, WriteMongodTransaction> getDeleteImplementation() {
      return new DeleteImplementation(mongodMetrics);
    }

    @Override
    public CommandImplementation<UpdateArgument, UpdateResult, WriteMongodTransaction> getUpdateImplementation() {
      return new UpdateImplementation(objectIdFactory, mongodMetrics);
    }

  }

  static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder<WriteMongodTransaction> {

    @Override
    public CommandImplementation<HandshakeArgument, Empty, WriteMongodTransaction> getHandshakeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetRBIDReply, WriteMongodTransaction> getReplSetGetRBIDImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetUpdatePositionArgument, Empty, WriteMongodTransaction> getReplSetUpdateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetElectArgument, ReplSetElectReply, WriteMongodTransaction> getReplSetElectImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, WriteMongodTransaction> getReplSetFreshImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, WriteMongodTransaction> getReplSetHeartbeatImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, WhatsMyUriReply, WriteMongodTransaction> getWhatsMyUriImplementation() {
      return NotImplementedCommandImplementation.build();
    }
  }

  static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<WriteMongodTransaction> {

    @Override
    public CommandImplementation<ApplyOpsArgument, ApplyOpsReply, WriteMongodTransaction> getApplyOpsImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, WriteMongodTransaction> getReplSetFreezeImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, IsMasterReply, WriteMongodTransaction> getIsMasterImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplicaSetConfig, WriteMongodTransaction> getReplSetGetConfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Empty, ReplSetGetStatusReply, WriteMongodTransaction> getReplSetGetStatusImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplicaSetConfig, Empty, WriteMongodTransaction> getReplSetInitiateImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<Boolean, Empty, WriteMongodTransaction> getReplSetMaintenanceImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetReconfigArgument, Empty, WriteMongodTransaction> getReplSetReconfigImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<ReplSetStepDownArgument, Empty, WriteMongodTransaction> getReplSetStepDownImplementation() {
      return NotImplementedCommandImplementation.build();
    }

    @Override
    public CommandImplementation<HostAndPort, ReplSetSyncFromReply, WriteMongodTransaction> getReplSetSyncFromImplementation() {
      return NotImplementedCommandImplementation.build();
    }

  }

  private static class ReplImplToCommandImplementationAdapter<A, R> implements
      CommandImplementation<A, R, WriteMongodTransaction> {

    private final ReplCommandImpl<A, R> replCommand;

    public ReplImplToCommandImplementationAdapter(ReplCommandImpl<A, R> replCommand) {
      super();
      this.replCommand = replCommand;
    }

    @Override
    public Status<R> apply(Request req, Command<? super A, ? super R> command, A arg,
        WriteMongodTransaction context) {
      return replCommand.apply(req, command, arg, context.getTorodTransaction());
    }
  }
}
