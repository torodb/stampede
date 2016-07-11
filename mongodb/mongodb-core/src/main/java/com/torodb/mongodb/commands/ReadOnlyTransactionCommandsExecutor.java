
package com.torodb.mongodb.commands;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

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
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.admin.ListCollectionsImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.CollStatsImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ListDatabasesImplementation;
import com.torodb.mongodb.commands.impl.general.FindImplementation;
import com.torodb.mongodb.core.ReadOnlyMongodTransaction;

/**
 *
 */
public class ReadOnlyTransactionCommandsExecutor implements CommandsExecutor<ReadOnlyMongodTransaction>{

    private final Set<Command<?,?>> supportedCommands;
    private final MapBasedCommandsExecutor<ReadOnlyMongodTransaction> delegate;

    @Inject
    ReadOnlyTransactionCommandsExecutor(MapFactory mapFactory) {
        ImmutableMap<Command<?, ?>, CommandImplementation> supportedCommandsMap = mapFactory.get();

        supportedCommands = Collections.unmodifiableSet(
                supportedCommandsMap.entrySet().stream()
                .filter((e) -> !(e.getValue() instanceof NotImplementedCommandImplementation))
                .map((e) -> e.getKey())
                .collect(Collectors.toSet())
        );

        delegate = MapBasedCommandsExecutor.<ReadOnlyMongodTransaction>builder()
                .addImplementations(supportedCommandsMap.entrySet())
                .build();
    }

    Set<Command<?, ?>> getSupportedCommands() {
        return supportedCommands;
    }

    @Override
    public <Arg, Result> Status<Result> execute(Request request, Command<? super Arg, ? super Result> command, Arg arg, ReadOnlyMongodTransaction context) {
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

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {

        @Inject
        private ListCollectionsImplementation listCollectionsImplementation;

        @Override
        public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, ? super ReadOnlyMongodTransaction> getListCollectionsImplementation() {
            return listCollectionsImplementation;
        }

        @Override
        public CommandImplementation<Empty, Empty, ReadOnlyMongodTransaction> getDropDatabaseImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CollectionCommandArgument, Empty, ReadOnlyMongodTransaction> getDropCollectionImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateCollectionArgument, Empty, ReadOnlyMongodTransaction> getCreateCollectionImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ListIndexesArgument, ListIndexesResult, ReadOnlyMongodTransaction> getListIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, ReadOnlyMongodTransaction> getCreateIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {

        @Override
        public CommandImplementation<CountArgument, Long, ReadOnlyMongodTransaction> getCountImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {

        @Override
        public CommandImplementation<Empty, String, ReadOnlyMongodTransaction> getGetNonceImplementation() {
            return NotImplementedCommandImplementation.build();
        }
    }

    static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {
        
        private final ListDatabasesImplementation listDatabasesImplementation;
        private final CollStatsImplementation collStatsImplementation;
        
        @Inject
        public MyDiagnosticCommandsImplementationBuilder(ListDatabasesImplementation listDatabasesImplementation,
                CollStatsImplementation collStatsImplementation) {
            super();
            this.listDatabasesImplementation = listDatabasesImplementation;
            this.collStatsImplementation = collStatsImplementation;
        }

        @Override
        public CommandImplementation<CollStatsArgument, CollStatsReply, ? super ReadOnlyMongodTransaction> getCollStatsImplementation() {
            return collStatsImplementation;
        }

        @Override
        public CommandImplementation<Empty, ListDatabasesReply, ? super ReadOnlyMongodTransaction> getListDatabasesImplementation() {
            return listDatabasesImplementation;
        }

        @Override
        public CommandImplementation<Empty, BuildInfoResult, ReadOnlyMongodTransaction> getBuildInfoImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ServerStatusArgument, ServerStatusReply, ReadOnlyMongodTransaction> getServerStatusImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<GetLogArgument, GetLogReply, ReadOnlyMongodTransaction> getGetLogImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, Empty, ReadOnlyMongodTransaction> getPingCommandImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {
        @Inject
        private FindImplementation findImpl;

        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, ReadOnlyMongodTransaction> getGetLastErrrorImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<FindArgument, FindResult, ? super ReadOnlyMongodTransaction> getFindImplementation() {
            return findImpl;
        }

        @Override
        public CommandImplementation<InsertArgument, InsertResult, ReadOnlyMongodTransaction> getInsertImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<DeleteArgument, Long, ReadOnlyMongodTransaction> getDeleteImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<UpdateArgument, UpdateResult, ReadOnlyMongodTransaction> getUpdateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {

        @Override
        public CommandImplementation<HandshakeArgument, Empty, ReadOnlyMongodTransaction> getHandshakeImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplSetGetRBIDReply, ReadOnlyMongodTransaction> getReplSetGetRBIDImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetUpdatePositionArgument, Empty, ReadOnlyMongodTransaction> getReplSetUpdateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetElectArgument, ReplSetElectReply, ReadOnlyMongodTransaction> getReplSetElectImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply, ReadOnlyMongodTransaction> getReplSetFreshImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply, ReadOnlyMongodTransaction> getReplSetHeartbeatImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, WhatsMyUriReply, ReadOnlyMongodTransaction> getWhatsMyUriImplementation() {
            return NotImplementedCommandImplementation.build();
        }
    }

    static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<ReadOnlyMongodTransaction> {
        
        @Override
        public CommandImplementation<ApplyOpsArgument, ApplyOpsReply, ReadOnlyMongodTransaction> getApplyOpsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply, ReadOnlyMongodTransaction> getReplSetFreezeImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, IsMasterReply, ReadOnlyMongodTransaction> getIsMasterImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplicaSetConfig, ReadOnlyMongodTransaction> getReplSetGetConfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplSetGetStatusReply, ReadOnlyMongodTransaction> getReplSetGetStatusImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplicaSetConfig, Empty, ReadOnlyMongodTransaction> getReplSetInitiateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Boolean, Empty, ReadOnlyMongodTransaction> getReplSetMaintenanceImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetReconfigArgument, Empty, ReadOnlyMongodTransaction> getReplSetReconfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetStepDownArgument, Empty, ReadOnlyMongodTransaction> getReplSetStepDownImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<HostAndPort, ReplSetSyncFromReply, ReadOnlyMongodTransaction> getReplSetSyncFromImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }
}
