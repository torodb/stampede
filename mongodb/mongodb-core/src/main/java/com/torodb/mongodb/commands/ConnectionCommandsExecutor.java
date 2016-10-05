
package com.torodb.mongodb.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListCollectionsCommand.ListCollectionsResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.ListIndexesCommand.ListIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.RenameCollectionCommand.RenameCollectionArgument;
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
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.internal.ReplSetHeartbeatReply;
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
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.authentication.GetNonceImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.BuildInfoImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.GetLogImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.PingImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ServerStatusImplementation;
import com.torodb.mongodb.commands.impl.internal.WhatsMyUriImplementation;
import com.torodb.mongodb.commands.impl.replication.IsMasterImplementation;
import com.torodb.mongodb.core.MongodConnection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class ConnectionCommandsExecutor {

    private final ImmutableMap<Command<?,?>, CommandImplementation<?,?, ? super MongodConnection>> map;
    private final Set<Command<?, ?>> supportedCommands;

    @Inject
    ConnectionCommandsExecutor(MapFactory mapFactory) {
        map = mapFactory.get();

        supportedCommands = Collections.unmodifiableSet(
                map.entrySet().stream()
                .filter((e) -> !(e.getValue() instanceof NotImplementedCommandImplementation))
                .map((e) -> e.getKey())
                .collect(Collectors.toSet())
        );
    }

    @DoNotChange
    Set<Command<?, ?>> getSupportedCommands() {
        return supportedCommands;
    }

    public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodConnection>> getMap() {
        return map;
    }
    
    static class MapFactory implements Supplier<ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super MongodConnection>>> {

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
        public ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super MongodConnection>> get() {
            MongoDb30CommandsImplementationBuilder<MongodConnection> implBuilder = new MongoDb30CommandsImplementationBuilder<>(
                    adminBuilder, aggregationBuilder, authenticationCommandsImplementationsBuilder, diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
            );

            ImmutableMap.Builder<Command<?,?>, CommandImplementation<?, ?, ? super MongodConnection>> builder = ImmutableMap.builder();
            for (Entry<Command<?,?>, CommandImplementation<?, ?, ? super MongodConnection>> entry : implBuilder) {
                builder.put(entry.getKey(), entry.getValue());
            }

            return builder.build();
        }

    }

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<MongodConnection> {
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
        private final GetNonceImplementation getNonceImplementation;

        @Inject
        public MyAuthenticationCommandsImplementationsBuilder(GetNonceImplementation getNonceImplementation) {
            this.getNonceImplementation = getNonceImplementation;
        }

        @Override
        public GetNonceImplementation getGetNonceImplementation() {
            return getNonceImplementation;
        }

    }

    static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<MongodConnection> {

        private final PingImplementation ping;
        private final GetLogImplementation getLog;
        private final BuildInfoImplementation buildInfo;
        private final ServerStatusImplementation serverStatusImplementation;

        @Inject
        public MyDiagnosticCommandsImplementationBuilder(PingImplementation ping, 
                GetLogImplementation getLog, 
                BuildInfoImplementation buildInfo,
                ServerStatusImplementation serverStatusImplementation) {
            this.ping = ping;
            this.getLog = getLog;
            this.buildInfo = buildInfo;
            this.serverStatusImplementation = serverStatusImplementation;
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
            return buildInfo;
        }

        @Override
        public CommandImplementation<ServerStatusArgument, ServerStatusReply, MongodConnection> getServerStatusImplementation() {
            return serverStatusImplementation;
        }

        @Override
        public CommandImplementation<GetLogArgument, GetLogReply, MongodConnection> getGetLogImplementation() {
            return getLog;
        }

        @Override
        public CommandImplementation<Empty, Empty, MongodConnection> getPingCommandImplementation() {
            return ping;
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
        private final WhatsMyUriImplementation whatsMyUriImpl;
        
        @Inject
        public MyInternalCommandsImplementationsBuilder(WhatsMyUriImplementation whatsMyUriImpl) {
            this.whatsMyUriImpl = whatsMyUriImpl;
        }

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
            return whatsMyUriImpl;
        }
    }

    static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder<MongodConnection> {
        private final IsMasterImplementation isMasterImpl;

        @Inject
        public MyReplCommandsImplementationsBuilder(IsMasterImplementation isMasterImpl) {
            this.isMasterImpl = isMasterImpl;
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
            return isMasterImpl;
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
