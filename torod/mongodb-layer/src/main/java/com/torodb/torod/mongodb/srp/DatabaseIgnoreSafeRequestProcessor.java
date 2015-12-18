
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.*;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.AdminCommands.AdminCommandsImplementationsBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateCollectionCommand.CreateCollectionArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.admin.CreateIndexesCommand.CreateIndexesArgument;
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
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.Empty;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.annotations.Local;
import com.torodb.torod.mongodb.commands.NotImplementedCommandImplementation;
import com.torodb.torod.mongodb.commands.ToroV30CommandTool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.bson.BsonDocument;

/**
 * This {@linkplain SafeRequestProcessor} ignores requests on not supported databases.
 * <p/>
 * Right now, ToroDB only supports one database whose name is decided when the
 * service start. In general local requests can only be accepted if they are
 * executed on the supported database, but some commands can relax that restriction
 */
@Singleton @Local
@SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
public class DatabaseIgnoreSafeRequestProcessor extends DecoratorSafeRequestProcessor {

    private final CommandsExecutor myCommandExecutor;
    private final String supportedDatabase;
    private final OptimeClock optimeClock;

    @Inject
    public DatabaseIgnoreSafeRequestProcessor(
            ToroSafeRequestProcessor delegate,
            @DatabaseName String supportedDatabase,
            OptimeClock optimeClock,
            ToroV30CommandTool toroSafeCommandTool) {
        super(delegate);
        this.supportedDatabase = supportedDatabase;
        this.optimeClock = optimeClock;

        Predicate<Map.Entry<Command,CommandImplementation>> implementedCommandFunction = new Predicate<Map.Entry<Command,CommandImplementation>>() {

            @Override
            public boolean apply(@Nonnull Map.Entry<Command,CommandImplementation> input) {
                return !(input.getValue() instanceof NotImplementedCommandImplementation);
            }
        };

        this.myCommandExecutor = MapBasedCommandsExecutor.builder()
                .addImplementations(Iterables.filter(new MyAdminCommandsImplementationBuilder(), implementedCommandFunction))
                .addImplementations(Iterables.filter(new MyAggregationCommandsImplementationBuilder(), implementedCommandFunction))
                .addImplementations(Iterables.filter(new MyDiagnosticCommandsImplementationBuilder(), implementedCommandFunction))
                .addImplementations(Iterables.filter(new MyGeneralCommandsImplementationBuilder(), implementedCommandFunction))
                .addImplementations(Iterables.filter(new MyInternalCommandsImplementationsBuilder(), implementedCommandFunction))
                .addImplementations(Iterables.filter(new MyReplCommandsImplementationsBuilder(), implementedCommandFunction))
                .build();
    }

    private boolean isAllowed(String database) {
        assert database != null : "only requests with database should be catched by this decorator";
        return database.equals(supportedDatabase);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        String database = deleteMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        if (!isAllowed(database)) {
            return Futures.immediateFuture(
                    new SimpleWriteOpResult(ErrorCode.OK, null, null, optimeClock.tick())
            );
        }
        return super.delete(request, deleteMessage);
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage update)
            throws MongoException {
        String database = update.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        if (!isAllowed(database)) {
            return Futures.immediateFuture(
                    new UpdateOpResult(0, 0, false, ErrorCode.OK, null, null, optimeClock.tick())
            );
        }
        return super.update(request, update);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        String database = insertMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        if (!isAllowed(database)) {
            return Futures.immediateFuture(
                    new SimpleWriteOpResult(ErrorCode.OK, null, null, optimeClock.tick())
            );
        }
        return super.insert(request, insertMessage);
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest queryMessage) throws
            MongoException {
        String database = queryMessage.getDatabase();
        assert database != null;
        assert database.equals(request.getDatabase());
        if (!isAllowed(database)) {
            return new ReplyMessage(request.getRequestId(), 0, queryMessage.getNumberToSkip(), ImmutableList.<BsonDocument>of());
        }
        return super.query(request, queryMessage);
    }

    @Override
    public <Arg, Result> CommandReply<Result> execute(Command<? super Arg, ? super Result> command, CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        String database = request.getDatabase();
        assert database != null;
        if (!isAllowed(database)) {
            try {
                return myCommandExecutor.execute(command, request);
            } catch (CommandNotSupportedException ex) {
                //in this case, we fall back on the usual implementation
            }
        }
        return super.execute(command, request);
    }

    static class MyAdminCommandsImplementationBuilder extends AdminCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<ListCollectionsArgument, ListCollectionsResult> getListCollectionsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, Empty> getDropDatabaseImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CollectionCommandArgument, Empty> getDropCollectionImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateCollectionArgument, Empty> getCreateCollectionImplementation() {
            return new CommandImplementation<CreateCollectionArgument, Empty>() {

                @Override
                public CommandResult<Empty> apply(Command<? super CreateCollectionArgument, ? super Empty> command, CommandRequest<CreateCollectionArgument> req)
                        throws MongoException {
                    return new NonWriteCommandResult<Empty>(Empty.getInstance());
                }
            };
        }

        @Override
        public CommandImplementation<ListIndexesArgument, ListIndexesResult> getListIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<CreateIndexesArgument, Empty> getCreateIndexesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyAggregationCommandsImplementationBuilder extends AggregationCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<CountArgument, Long> getCountImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyDiagnosticCommandsImplementationBuilder extends DiagnosticCommandsImplementationsBuilder {

        @Override
        public CommandImplementation<CollStatsArgument, CollStatsReply> getCollStatsImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, ListDatabasesReply> getListDatabasesImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<Empty, BuildInfoResult> getBuildInfoImplementation() {
            return NotImplementedCommandImplementation.build();
        }

    }

    static class MyGeneralCommandsImplementationBuilder extends GeneralCommandsImplementationsBuilder {
        @Override
        public CommandImplementation<GetLastErrorArgument, GetLastErrorReply> getGetLastErrrorImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<InsertArgument, InsertResult> getInsertImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<DeleteArgument, Long> getDeleteImplementation() {
            return NotImplementedCommandImplementation.build();
        }

        @Override
        public CommandImplementation<UpdateArgument, UpdateResult> getUpdateImplementation() {
            return NotImplementedCommandImplementation.build();
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
