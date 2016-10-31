
package com.torodb.mongodb.commands;

import java.util.Set;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CollModCommand.CollModArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CollModCommand.CollModResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.DropIndexesCommand.DropIndexesResult;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import com.torodb.core.annotations.DoNotChange;
import com.torodb.mongodb.commands.impl.NotImplementedCommandImplementation;
import com.torodb.mongodb.commands.impl.admin.CreateCollectionImplementation;
import com.torodb.mongodb.commands.impl.admin.CreateIndexesImplementation;
import com.torodb.mongodb.commands.impl.admin.DropCollectionImplementation;
import com.torodb.mongodb.commands.impl.admin.DropDatabaseImplementation;
import com.torodb.mongodb.commands.impl.admin.DropIndexesImplementation;
import com.torodb.mongodb.commands.impl.general.DeleteImplementation;
import com.torodb.mongodb.commands.impl.general.InsertImplementation;
import com.torodb.mongodb.commands.impl.general.UpdateImplementation;
import com.torodb.mongodb.core.MongodMetrics;
import com.torodb.mongodb.core.WriteMongodTransaction;
import com.torodb.mongodb.language.ObjectIdFactory;

/**
 *
 */
@Singleton
public class WriteTransactionImplementations {

    private final ImmutableMap<Command<?,?>, CommandImplementation<?,?, ? super WriteMongodTransaction>> map;

    @Inject
    WriteTransactionImplementations(Injector injector) {
        this(new MapFactory(injector));
    }

    protected WriteTransactionImplementations(Supplier<ImmutableMap<Command<?,?>, CommandImplementation<?, ?, ? super WriteMongodTransaction>>> mapFactory) {
        map = mapFactory.get();
    }

    @DoNotChange
    Set<Command<?, ?>> getSupportedCommands() {
        return map.keySet();
    }

    public ImmutableMap<Command<?, ?>, CommandImplementation<?, ?, ? super WriteMongodTransaction>> getMap() {
        return map;
    }

    static class MapFactory extends AbstractCommandMapFactory<WriteMongodTransaction> {
        @Inject
        public MapFactory(Injector injector) {
            super(
                    new MyAdminCommandsImplementationBuilder(),
                    new MyAggregationCommandsImplementationBuilder(),
                    new MyAuthenticationCommandsImplementationsBuilder(),
                    new MyDiagnosticCommandsImplementationBuilder(),
                    new MyGeneralCommandsImplementationBuilder(injector),
                    new MyInternalCommandsImplementationsBuilder(),
                    new MyReplCommandsImplementationsBuilder()
            );
        }
    }
    
    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder<WriteMongodTransaction> {

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
            return new DropDatabaseImplementation();
        }

        @Override
        public CommandImplementation<CollectionCommandArgument, Empty, WriteMongodTransaction> getDropCollectionImplementation() {
            return new DropCollectionImplementation();
        }

        @Override
        public CommandImplementation<CreateCollectionArgument, Empty, WriteMongodTransaction> getCreateCollectionImplementation() {
            return new CreateCollectionImplementation();
        }

        @Override
        public CommandImplementation<ListIndexesArgument, ListIndexesResult, WriteMongodTransaction> getListIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, CreateIndexesResult, WriteMongodTransaction> getCreateIndexesImplementation() {
            return new CreateIndexesImplementation();
        }

        @Override
        public CommandImplementation<DropIndexesArgument, DropIndexesResult, ? super WriteMongodTransaction> getDropIndexesImplementation() {
            return new DropIndexesImplementation();
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
}
