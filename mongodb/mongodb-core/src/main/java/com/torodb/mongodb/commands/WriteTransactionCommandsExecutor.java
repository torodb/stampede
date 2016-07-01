
package com.torodb.mongodb.commands;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.CountCommand.CountArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.authentication.AuthenticationCommands.AuthenticationCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.BuildInfoCommand.BuildInfoResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.GetLogArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.GetLogCommand.GetLogReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.FindCommand.FindResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.HandshakeCommand.HandshakeArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.InternalCommands.InternalCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetElectCommand.ReplSetElectArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetElectCommand.ReplSetElectReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetFreshCommand.ReplSetFreshArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetFreshCommand.ReplSetFreshReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetGetRBIDCommand.ReplSetGetRBIDReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetHeartbeatCommand.ReplSetHeartbeatReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetUpdatePositionCommand.ReplSetUpdatePositionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.WhatsMyUriCommand.WhatsMyUriReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ApplyOpsCommand.ApplyOpsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ApplyOpsCommand.ApplyOpsReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.IsMasterCommand.IsMasterReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplCommands.ReplCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetFreezeCommand.ReplSetFreezeArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetFreezeCommand.ReplSetFreezeReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetReconfigCommand.ReplSetReconfigArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.ReplicaSetConfig;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.impl.MapBasedCommandsExecutor;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.admin.DropCollectionImplementation;
import com.torodb.mongodb.commands.impl.admin.DropDatabaseImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.PingImplementation;
import com.torodb.mongodb.commands.impl.general.DeleteImplementation;
import com.torodb.mongodb.commands.impl.general.InsertImplementation;
import com.torodb.mongodb.commands.impl.general.UpdateImplementation;
import com.torodb.mongodb.core.WriteMongodTransaction;

/**
 *
 */
@Singleton
public class WriteTransactionCommandsExecutor implements CommandsExecutor<WriteMongodTransaction> {

    private final Set<Command<?,?>> supportedCommands;
    private final MapBasedCommandsExecutor<WriteMongodTransaction> delegate;

    @Inject
    WriteTransactionCommandsExecutor(MapFactory mapFactory) {
        ImmutableMap<Command<?, ?>, CommandImplementation> supportedCommandsMap = mapFactory.get();

        supportedCommands = Collections.unmodifiableSet(
                supportedCommandsMap.entrySet().stream()
                .filter((e) -> !(e.getValue() instanceof NotImplementedCommandImplementation))
                .map((e) -> e.getKey())
                .collect(Collectors.toSet())
        );

        delegate = MapBasedCommandsExecutor.<WriteMongodTransaction>builder()
                .addImplementations(supportedCommandsMap.entrySet())
                .build();
    }

    @DoNotChange
    Set<Command<?, ?>> getSupportedCommands() {
        return supportedCommands;
    }

    @Override
    public <Arg, Result> Status<Result> execute(Request request, Command<? super Arg, ? super Result> command, Arg arg, WriteMongodTransaction context) {
        return delegate.execute(request, command, arg, context);
    }

    static class MapFactory implements Supplier<ImmutableMap<Command<?,?>, CommandImplementation>> {

        private final MyAdminCommandsImplementationBuilder adminBuilder;
        private final MyAggregationCommandsImplementationBuilder aggregationBuilder;
        private final MyAuthenticationCommandsImplementationsBuilder authenticationCommandsImplementationsBuilder;
        private final MyDiagnosticCommandsImplementationBuilder diagnosticBuilder;
        private final MyGeneralCommandsImplementationBuilder generalBuilder;
        private final MyInternalCommandsImplementationsBuilder internalBuilder;
        private final MyReplCommandsImplementationsBuilder replBuilder;

        @Inject
        public MapFactory(
                MyAdminCommandsImplementationBuilder adminBuilder,
                MyAggregationCommandsImplementationBuilder aggregationBuilder,
                MyAuthenticationCommandsImplementationsBuilder authenticationCommandsImplementationsBuilder,
                MyDiagnosticCommandsImplementationBuilder diagnosticBuilder,
                MyGeneralCommandsImplementationBuilder generalBuilder,
                MyInternalCommandsImplementationsBuilder internalBuilder,
                MyReplCommandsImplementationsBuilder replBuilder) {
            this.adminBuilder = adminBuilder;
            this.aggregationBuilder = aggregationBuilder;
            this.authenticationCommandsImplementationsBuilder = authenticationCommandsImplementationsBuilder;
            this.diagnosticBuilder = diagnosticBuilder;
            this.generalBuilder = generalBuilder;
            this.internalBuilder = internalBuilder;
            this.replBuilder = replBuilder;
        }

        @Override
        public ImmutableMap<Command<?,?>, CommandImplementation> get() {
            MongoDb30CommandsImplementationBuilder implBuilder = new MongoDb30CommandsImplementationBuilder(
                    adminBuilder, aggregationBuilder, authenticationCommandsImplementationsBuilder, diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
            );

            ImmutableMap.Builder<Command<?,?>, CommandImplementation> builder = ImmutableMap.builder();
            for (Entry<Command<?,?>, CommandImplementation> entry : implBuilder) {
                builder.put(entry.getKey(), entry.getValue());
            }

            return builder.build();
        }

    }

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<WriteMongodTransaction> {
        private final DropCollectionImplementation dropCollectionImplementation;
        private final DropDatabaseImplementation dropDatabaseImplementation;
        
        @Inject
        public MyAdminCommandsImplementationBuilder(
                DropCollectionImplementation dropCollectionImplementation,
                DropDatabaseImplementation dropDatabaseImplementation) {
            super();
            this.dropCollectionImplementation = dropCollectionImplementation;
            this.dropDatabaseImplementation = dropDatabaseImplementation;
        }

        @Override
        public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, WriteMongodTransaction> getListCollectionsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, Empty, WriteMongodTransaction> getDropDatabaseImplementation() {
            return dropDatabaseImplementation;
        }

        @Override
        public CommandImplementation<CollectionCommandArgument, Empty, WriteMongodTransaction> getDropCollectionImplementation() {
            return dropCollectionImplementation;
        }

        @Override
        public CommandImplementation<CreateCollectionArgument, Empty, WriteMongodTransaction> getCreateCollectionImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ListIndexesArgument, ListIndexesResult, WriteMongodTransaction> getListIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, WriteMongodTransaction> getCreateIndexesImplementation() {
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

        private final PingImplementation ping;

        @Inject
        public MyDiagnosticCommandsImplementationBuilder(PingImplementation ping) {
            this.ping = ping;
        }

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
        private final InsertImplementation insertImpl;
        private final DeleteImplementation deleteImpl;
        private final UpdateImplementation updateImpl;

        @Inject
        public MyGeneralCommandsImplementationBuilder(
                InsertImplementation insertImpl,
                DeleteImplementation deleteImpl,
                UpdateImplementation updateImpl) {
            super();
            this.insertImpl = insertImpl;
            this.deleteImpl = deleteImpl;
            this.updateImpl = updateImpl;
        }

        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, WriteMongodTransaction> getGetLastErrrorImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<InsertArgument, InsertResult, WriteMongodTransaction> getInsertImplementation() {
            return insertImpl;
        }

        @Override
        public CommandImplementation<FindArgument, FindResult, ? super WriteMongodTransaction> getFindImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<DeleteArgument, Long, WriteMongodTransaction> getDeleteImplementation() {
            return deleteImpl;
        }

        @Override
        public CommandImplementation<UpdateArgument, UpdateResult, WriteMongodTransaction> getUpdateImplementation() {
            return updateImpl;
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
}
