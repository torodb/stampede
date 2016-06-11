
package com.torodb.torod.mongodb.commands.impl.general;

import com.eightkdata.mongowp.server.api.Connection;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.WriteConcern;
import com.eightkdata.mongowp.WriteConcern.SyncMode;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.server.api.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.ReplInterface.MemberStateInterface;
import com.torodb.torod.mongodb.repl.ReplInterface.PrimaryStateInterface;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class GetLastErrorImplementation implements CommandImplementation<GetLastErrorArgument, GetLastErrorReply>{

    @Override
    public CommandResult<GetLastErrorReply> apply(
            Command<? super GetLastErrorArgument, ? super GetLastErrorReply> command,
            CommandRequest<GetLastErrorArgument> req) throws MongoException {
        return new NonWriteCommandResult<>(getResult(command, req));
    }

    public GetLastErrorReply getResult(
            Command<? super GetLastErrorArgument, ? super GetLastErrorReply> command,
            CommandRequest<GetLastErrorArgument> req) throws MongoException {

        GetLastErrorArgument arg = req.getCommandArgument();

        RequestContext context = RequestContext.getFrom(req);

        String supportedDatabase = context.getSupportedDatabase();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        Connection connection = req.getConnection();

        WriteOpResult writeOpResult;
        Future<? extends WriteOpResult> lastWriteOpFuture
                = connection.getAppliedLastWriteOp();
        boolean noWriteOpYet = lastWriteOpFuture == null;
        try {
            if (lastWriteOpFuture == null) {
                writeOpResult = new SimpleWriteOpResult(
                        ErrorCode.OK,
                        null,
                        null,
                        null,
                        new OpTime(0, 0)
                );
            }
            else {
                if (lastWriteOpFuture.isDone()) {
                    writeOpResult = lastWriteOpFuture.get();
                }
                else {
                    writeOpResult = null;
                }
            }
        }
        catch (InterruptedException | CancellationException |
                ExecutionException ex) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Error while waiting for last write op",
                    ex
            );
        }

        if (arg.getBadGLE() != null) {
            assert arg.getBadGLEErrorCode() != null;
            assert arg.getBadGLEMessage() != null;

            return new GetLastErrorReply(
                    arg.getBadGLEErrorCode(),
                    arg.getBadGLEMessage(),
                    connection.getConnectionId(),
                    writeOpResult,
                    arg,
                    null
            );
        }

        if (arg.getWElectionId() != null && arg.getWOpTime() != null) {
            //TODO: Support sharding
            throw new CommandFailed(
                    command.getCommandName(),
                    "GetLastError with wElectionId and wOpTime belongs to "
                            + "sharding protocol, which is not supported yet"
            );
        }

        WriteConcern wc = arg.getWriteConcern();
        WriteConcernEnforcementResult awaitReplication;

        if (!noWriteOpYet) {
            assert wc != null;
            if (writeOpResult == null && (wc.getSyncMode() == SyncMode.FSYNC || wc.getSyncMode() == SyncMode.JOURNAL)) {
                assert lastWriteOpFuture != null;
                try {
                    writeOpResult = lastWriteOpFuture.get();
                } catch (InterruptedException |
                        ExecutionException ex) {
                    throw new CommandFailed(
                            command.getCommandName(),
                            "Error while waiting for last write op",
                            ex
                    );
                }
            }
            if (writeOpResult == null) {
                awaitReplication = new WriteConcernEnforcementResult(
                        wc,
                        null,
                        0,
                        null,
                        false,
                        null,
                        null
                );
            }
            else {
                ReplCoordinator replCoordinator = context.getReplicationCoordinator();

                try (MemberStateInterface freezeMemberState = replCoordinator.freezeMemberState(false)) {
                    if (freezeMemberState instanceof PrimaryStateInterface) {
                        PrimaryStateInterface psi = (PrimaryStateInterface) freezeMemberState;

                        awaitReplication = psi.awaitReplication(
                                writeOpResult.getOpTime(),
                                arg.getWriteConcern()
                        );
                    }
                    else {
                        //TODO: Dedice what to do with that
                        throw new CommandFailed(
                                command.getCommandName(),
                                "GetLastError is not supported right now on a non primary node"
                        );
                    }
                }
            }
        }
        else {

            awaitReplication = new WriteConcernEnforcementResult(
                    WriteConcern.acknowledged(),
                    null,
                    0,
                    null,
                    false,
                    null,
                    null
            );
        }

        return new GetLastErrorReply(
                arg.getBadGLEErrorCode(),
                arg.getBadGLEMessage(),
                connection.getConnectionId(),
                writeOpResult,
                arg,
                awaitReplication
        );
    }

}
