
package com.torodb.torod.mongodb.commands;

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
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.BuildInfoCommand.BuildInfoResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ListDatabasesCommand.ListDatabasesReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.ServerStatusCommand.ServerStatusReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.DeleteCommand.DeleteArgument;
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
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.mongodb.commands.impl.admin.*;
import com.torodb.torod.mongodb.commands.impl.aggregation.CountImplementation;
import com.torodb.torod.mongodb.commands.impl.diagnostic.CollStatsImplementation;
import com.torodb.torod.mongodb.commands.impl.diagnostic.ListDatabasesImplementation;
import com.torodb.torod.mongodb.commands.impl.diagnostic.ServerStatusImplementation;
import com.torodb.torod.mongodb.commands.impl.general.DeleteImplementation;
import com.torodb.torod.mongodb.commands.impl.general.GetLastErrorImplementation;
import com.torodb.torod.mongodb.commands.impl.general.InsertImplementation;
import com.torodb.torod.mongodb.commands.impl.general.UpdateImplementation;
import com.torodb.torod.mongodb.repl.ObjectIdFactory;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import java.util.Map.Entry;
import javax.inject.Inject;

/**
 * This utility class is used to create and list safe implementations of the
 * commands supported by ToroDB.
 * <p/>
 * As not all commands are implemented using a real
 * {@linkplain CommandImplementation}, it is needed to cheat the safe framework.
 */
public class ToroV30CommandTool {

    private final ImmutableMap<Command<?,?>, CommandImplementation> map;

    @Inject
    ToroV30CommandTool(MapFactory mapFactory) {
        this.map = mapFactory.get();
    }

    public ImmutableMap<Command<?,?>, CommandImplementation> getMap() {
        return map;
    }

    static class MapFactory implements Supplier<ImmutableMap<Command<?,?>, CommandImplementation>> {

        private final MyAdminCommandsImplementationBuilder adminBuilder;
        private final MyAggregationCommandsImplementationBuilder aggregationBuilder;
        private final MyDiagnosticCommandsImplementationBuilder diagnosticBuilder;
        private final MyGeneralCommandsImplementationBuilder generalBuilder;
        private final MyInternalCommandsImplementationsBuilder internalBuilder;
        private final MyReplCommandsImplementationsBuilder replBuilder;

        @Inject
        public MapFactory(
                MyAdminCommandsImplementationBuilder adminBuilder,
                MyAggregationCommandsImplementationBuilder aggregationBuilder,
                MyDiagnosticCommandsImplementationBuilder diagnosticBuilder,
                MyGeneralCommandsImplementationBuilder generalBuilder,
                MyInternalCommandsImplementationsBuilder internalBuilder,
                MyReplCommandsImplementationsBuilder replBuilder) {
            this.adminBuilder = adminBuilder;
            this.aggregationBuilder = aggregationBuilder;
            this.diagnosticBuilder = diagnosticBuilder;
            this.generalBuilder = generalBuilder;
            this.internalBuilder = internalBuilder;
            this.replBuilder = replBuilder;
        }

        @Override
        public ImmutableMap<Command<?,?>, CommandImplementation> get() {
            MongoDb30CommandsImplementationBuilder implBuilder = new MongoDb30CommandsImplementationBuilder(
                    adminBuilder, aggregationBuilder, diagnosticBuilder, generalBuilder, internalBuilder, replBuilder
            );

            ImmutableMap.Builder<Command<?,?>, CommandImplementation> builder = ImmutableMap.builder();
            for (Entry<Command<?,?>, CommandImplementation> entry : implBuilder) {
                builder.put(entry.getKey(), entry.getValue());
            }

            return builder.build();
        }

    }

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder {
        
        private final ListCollectionsImplementation listCollections;
        private final DropDatabaseImplementation dropDatabase;
        private final CreateIndexesImplementation createIndexes;
        private final ListIndexesImplementation listIndexes;

        @Inject
        public MyAdminCommandsImplementationBuilder(ListCollectionsImplementation listCollections, DropDatabaseImplementation dropDatabase, CreateIndexesImplementation createIndexes, ListIndexesImplementation listIndexes) {
            this.listCollections = listCollections;
            this.dropDatabase = dropDatabase;
            this.createIndexes = createIndexes;
            this.listIndexes = listIndexes;
        }

        @Override
        public CommandImplementation<ListCollectionsArgument, ListCollectionsResult> getListCollectionsImplementation() {
            return listCollections;
        }

        @Override
        public CommandImplementation<Empty, Empty> getDropDatabaseImplementation() {
            return dropDatabase;
        }

        @Override
        public CommandImplementation<CollectionCommandArgument, Empty> getDropCollectionImplementation() {
            return DropCollectionImplementation.INSTANCE;
        }

        @Override
        public CommandImplementation<CreateCollectionArgument, Empty> getCreateCollectionImplementation() {
            return CreateCollectionImplementation.INSTANCE;
        }

        @Override
        public CommandImplementation<ListIndexesArgument, ListIndexesResult> getListIndexesImplementation() {
            return listIndexes;
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, CreateIndexesResult> getCreateIndexesImplementation() {
            return createIndexes;
        }

    }

    static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder {

        private final CountImplementation countImplementation;

        @Inject
        public MyAggregationCommandsImplementationBuilder(CountImplementation countImplementation) {
            this.countImplementation = countImplementation;
        }

        @Override
        public CommandImplementation<CountArgument, Long> getCountImplementation() {
            return countImplementation;
        }

    }

    static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder {
        private final ServerStatusImplementation serverStatusImplementation;
        private final CollStatsImplementation collStatsImplementation;

        @Inject
        public MyDiagnosticCommandsImplementationBuilder(ServerStatusImplementation serverStatusImplementation, CollStatsImplementation collStatsImplementation) {
            this.serverStatusImplementation = serverStatusImplementation;
            this.collStatsImplementation = collStatsImplementation;
        }

        @Override
        public CommandImplementation<CollStatsArgument, CollStatsReply> getCollStatsImplementation() {
            return collStatsImplementation;
        }

        @Override
        public CommandImplementation<Empty, ListDatabasesReply> getListDatabasesImplementation() {
            return ListDatabasesImplementation.INSTANCE;
        }

        @Override
        public CommandImplementation<Empty, BuildInfoResult> getBuildInfoImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ServerStatusArgument, ServerStatusReply> getServerStatusImplementation() {
            return serverStatusImplementation;
        }

    }

    static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder {
        private final WriteConcernToWriteFailModeFunction toWriteFailModeFunction;
        private final QueryCriteriaTranslator queryCriteriaTranslator;
        private final DocumentBuilderFactory documentBuilderFactory;
        private final ObjectIdFactory objectIdFactory;

        @Inject
        public MyGeneralCommandsImplementationBuilder(
                WriteConcernToWriteFailModeFunction toWriteFailModeFunction,
                QueryCriteriaTranslator queryCriteriaTranslator,
                DocumentBuilderFactory documentBuilderFactory, 
                ObjectIdFactory objectIdFactory) {
            this.toWriteFailModeFunction = toWriteFailModeFunction;
            this.queryCriteriaTranslator = queryCriteriaTranslator;
            this.documentBuilderFactory = documentBuilderFactory;
            this.objectIdFactory = objectIdFactory;
        }

        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply> getGetLastErrrorImplementation() {
            return new GetLastErrorImplementation();
        }

        @Override
        public CommandImplementation<InsertArgument, InsertResult> getInsertImplementation() {
            return new InsertImplementation(toWriteFailModeFunction);
        }

        @Override
        public CommandImplementation<DeleteArgument, Long> getDeleteImplementation() {
            return new DeleteImplementation(toWriteFailModeFunction, queryCriteriaTranslator);
        }

        @Override
        public CommandImplementation<UpdateArgument, UpdateResult> getUpdateImplementation() {
            return new UpdateImplementation(toWriteFailModeFunction, queryCriteriaTranslator, documentBuilderFactory, objectIdFactory);
        }

    }

    static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<HandshakeArgument, Empty> getHandshakeImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplSetGetRBIDReply> getReplSetGetRBIDImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetUpdatePositionArgument, Empty> getReplSetUpdateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetElectArgument, ReplSetElectReply> getReplSetElectImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetFreshArgument, ReplSetFreshReply> getReplSetFreshImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply> getReplSetHeartbeatImplementation() {
            return NotImplementedCommandImplementation.build();
        }
    }

    static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<ApplyOpsArgument, ApplyOpsReply> getApplyOpsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply> getReplSetFreezeImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, IsMasterReply> getIsMasterImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplicaSetConfig> getReplSetGetConfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ReplSetGetStatusReply> getReplSetGetStatusImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplicaSetConfig, Empty> getReplSetInitiateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Boolean, Empty> getReplSetMaintenanceImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetReconfigArgument, Empty> getReplSetReconfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetStepDownArgument, Empty> getReplSetStepDownImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<HostAndPort, ReplSetSyncFromReply> getReplSetSyncFromImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

}
