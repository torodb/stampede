
package com.torodb.mongodb.commands;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

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
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.admin.ListCollectionsImplementation;
import com.torodb.mongodb.commands.impl.admin.ListIndexesImplementation;
import com.torodb.mongodb.commands.impl.aggregation.CountImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.CollStatsImplementation;
import com.torodb.mongodb.commands.impl.diagnostic.ListDatabasesImplementation;
import com.torodb.mongodb.commands.impl.general.FindImplementation;
import com.torodb.mongodb.core.MongodTransaction;

/**
 * This class contains the implementations of all commands that can be executed on a read or write
 * transaction.
 */
public class GeneralTransactionImplementations {

    private final ImmutableMap<Command<?,?>, CommandImplementation<?,?, ? super MongodTransaction>> map;
    private final Set<Command<?, ?>> supportedCommands;
    
    @Inject
    GeneralTransactionImplementations(MapFactory mapFactory) {
        map = mapFactory.get();

        supportedCommands = Collections.unmodifiableSet(
                map.entrySet().stream()
                .filter((e) -> !(e.getValue() instanceof NotImplementedCommandImplementation))
                .map((e) -> e.getKey())
                .collect(Collectors.toSet())
        );
    }

    Set<Command<?, ?>> getSupportedCommands() {
        return supportedCommands;
    }

    public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> getMap() {
        return map;
    }

    static class MapFactory implements Supplier<ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super MongodTransaction>>> {

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
        public ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super MongodTransaction>> get() {
            MongoDb30CommandsImplementationBuilder<MongodTransaction> implBuilder = new MongoDb30CommandsImplementationBuilder<>(
                    adminBuilder, aggregationBuilder, authenticationCommandsImplementationsBuilder, diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
            );

            ImmutableMap.Builder<Command<?,?>, CommandImplementation<?, ?, ? super MongodTransaction>> builder = ImmutableMap.builder();
            for (Entry<Command<?, ?>, CommandImplementation<?, ?, ? super MongodTransaction>> entry : implBuilder) {
                builder.put(entry.getKey(), entry.getValue());
            }

            return builder.build();
        }

    }

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<MongodTransaction> {
        private final ListCollectionsImplementation listCollectionsImplementation;
        private final ListIndexesImplementation listIndexesImplementation;

        @Inject
        public MyAdminCommandsImplementationBuilder(ListCollectionsImplementation listCollectionsImplementation,
                ListIndexesImplementation listIndexesImplementation) {
            super();
            this.listCollectionsImplementation = listCollectionsImplementation;
            this.listIndexesImplementation = listIndexesImplementation;
        }

        @Override
        public CommandImplementation<ListCollectionsArgument, ListCollectionsResult, ? super MongodTransaction> getListCollectionsImplementation() {
            return listCollectionsImplementation;
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
            return listIndexesImplementation;
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, MongodTransaction> getCreateIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<RenameCollectionArgument, Empty, ? super MongodTransaction> getRenameCollectionImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder<MongodTransaction> {

        private final CountImplementation countImplementation;
        
        @Inject
        public MyAggregationCommandsImplementationBuilder(CountImplementation countImplementation) {
            this.countImplementation = countImplementation;
        }

        @Override
        public CommandImplementation<CountArgument, Long, ? super MongodTransaction> getCountImplementation() {
            return countImplementation;
        }

    }

    static class MyAuthenticationCommandsImplementationsBuilder extends AuthenticationCommandsImplementationsBuilder<MongodTransaction> {

        @Override
        public CommandImplementation<Empty, String, MongodTransaction> getGetNonceImplementation() {
            return NotImplementedCommandImplementation.build();
        }
    }

    static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder<MongodTransaction> {
        
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
        public CommandImplementation<CollStatsArgument, CollStatsReply, ? super MongodTransaction> getCollStatsImplementation() {
            return collStatsImplementation;
        }

        @Override
        public CommandImplementation<Empty, ListDatabasesReply, ? super MongodTransaction> getListDatabasesImplementation() {
            return listDatabasesImplementation;
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
        @Inject
        private FindImplementation findImpl;

        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply, MongodTransaction> getGetLastErrrorImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<FindArgument, FindResult, ? super MongodTransaction> getFindImplementation() {
            return findImpl;
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
