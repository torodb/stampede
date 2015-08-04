
package com.torodb.torod.mongodb.standard;

import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.GroupedCommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.MapBasedCommandsExecutor;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.MongoDb30Commands.MongoDb30CommandsImplementationBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.AggregationCommands.AggregationCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.CountCommand.CountArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.aggregation.CountCommand.CountReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.CollStatsCommand.CollStatsReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.diagnostic.DiagnosticCommands.DiagnosticCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GeneralCommands.GeneralCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertReply;
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
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetGetConfigCommand.ReplSetGetConfigReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetGetStatusCommand.ReplSetGetStatusReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetInitiateCommand.ReplSetInitiateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetMaintenanceCommand.ReplSetMaintenanceArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetReconfigCommand.ReplSetReconfigArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetStepDownCommand.ReplSetStepDownArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetSyncFromCommand.ReplSetSyncFromArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.repl.ReplSetSyncFromCommand.ReplSetSyncFromReply;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.mongodb.standard.commands.NotImplementedCommandImplementation;
import com.torodb.torod.mongodb.standard.commands.general.GetLastErrorImplementation;
import com.torodb.torod.mongodb.standard.commands.general.InsertImplementation;
import com.torodb.torod.mongodb.unsafe.UnsafeCommandsExecutorAdaptor;
import javax.inject.Inject;

/**
 *
 */
public class StandardCommandsExecutor extends GroupedCommandsExecutor {

    @Inject
    public StandardCommandsExecutor(
            QueryCommandProcessor unsafeCommandProcessor) {
        super(getSubExecutors(unsafeCommandProcessor));
    }

    public static ImmutableList<CommandsExecutor> getSubExecutors(
            QueryCommandProcessor unsafeCommandProcessor) {
        return ImmutableList.<CommandsExecutor>builder()
                .add(createSafeCommandsExecutor())
                .add(new UnsafeCommandsExecutorAdaptor(unsafeCommandProcessor))
                .build();
    }

    public static CommandsExecutor createSafeCommandsExecutor() {
        MongoDb30CommandsImplementationBuilder implBuilder = new MongoDb30CommandsImplementationBuilder(
                new MyAggregationCommandsImplementationBuilder(),
                new MyDiagnosticCommandsImplementationBuilder(),
                new MyGeneralCommandsImplementationBuilder(),
                new MyInternalCommandsImplementationsBuilder(),
                new MyReplCommandsImplementationsBuilder()
        );

        return MapBasedCommandsExecutor.fromLibraryBuilder(StandardCommandsLibrary.getSafeLibrary())
                .addImplementations(implBuilder)
                .build();
    }

    private static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<CountArgument, CountReply> getCountImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    private static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<CollStatsArgument, CollStatsReply> getCollStatsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    private static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply> getGetLastErrrorImplementation() {
            return new GetLastErrorImplementation();
        }

        @Override
        public CommandImplementation<InsertArgument, InsertReply> getInsertImplementation() {
            return new InsertImplementation();
        }

    }

    private static class MyInternalCommandsImplementationsBuilder extends InternalCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<HandshakeArgument, SimpleReply> getHandshakeImplementation() {
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
        public CommandImplementation<SimpleArgument, ReplSetGetRBIDReply> getReplSetGetRBIDImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetHeartbeatArgument, ReplSetHeartbeatReply> getReplSetHeartbeatImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetUpdatePositionArgument, SimpleReply> getReplSetUpdateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    private static class MyReplCommandsImplementationsBuilder extends ReplCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<ApplyOpsArgument, ApplyOpsReply> getApplyOpsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<SimpleArgument, IsMasterReply> getIsMasterImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetFreezeArgument, ReplSetFreezeReply> getReplSetFreezeImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<SimpleArgument, ReplSetGetConfigReply> getReplSetGetConfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<SimpleArgument, ReplSetGetStatusReply> getReplSetGetStatusImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetInitiateArgument, SimpleReply> getReplSetInitiateImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetMaintenanceArgument, SimpleReply> getReplSetMaintenanceImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetReconfigArgument, SimpleReply> getReplSetReconfigImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetStepDownArgument, SimpleReply> getReplSetStepDownImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<ReplSetSyncFromArgument, ReplSetSyncFromReply> getReplSetSyncFromImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

}
