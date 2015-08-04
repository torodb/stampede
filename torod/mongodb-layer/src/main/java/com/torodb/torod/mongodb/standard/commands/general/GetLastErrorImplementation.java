
package com.torodb.torod.mongodb.standard.commands.general;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.Connection;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.GetLastErrorReply;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.GetLastErrorCommand.WriteConcernEnforcementResult;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.mongodb.WriteConcern;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.repl.ReplicationCoordinator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class GetLastErrorImplementation implements CommandImplementation<GetLastErrorArgument, GetLastErrorReply>{

    @Override
    public GetLastErrorReply apply(
            Command<? extends GetLastErrorArgument, ? extends GetLastErrorReply> command,
            CommandRequest<GetLastErrorArgument> req) throws MongoServerException {

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
        boolean noWriteOpYet;
        try {
            Future<? extends WriteOpResult> lastWriteOpFuture 
                    = connection.getLastWriteOp();
            noWriteOpYet = lastWriteOpFuture == null;
            if (noWriteOpYet) {
                writeOpResult = new SimpleWriteOpResult(
                        ErrorCode.OK,
                        null,
                        null,
                        null,
                        new OpTime(0, 0)
                );
            }
            else {
                writeOpResult = lastWriteOpFuture.get();
            }
        }
        catch (InterruptedException ex) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Error while waiting for last write op",
                    ex
            );
        }
        catch (CancellationException ex) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Error while waiting for last write op",
                    ex
            );
        }
        catch (ExecutionException ex) {
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
                    command,
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

        WriteConcernEnforcementResult awaitReplication;
        if (!noWriteOpYet) {
            ReplicationCoordinator replCoordinator = context.getReplicationCoordinator();

            awaitReplication = replCoordinator.awaitReplication(
                    writeOpResult.getOptime(),
                    arg.getWriteConcern()
            );
        }
        else {
            awaitReplication = new WriteConcernEnforcementResult(
                    WriteConcern.ACKNOWLEDGED,
                    null,
                    0,
                    null,
                    false,
                    null,
                    null
            );
        }

        return new GetLastErrorReply(
                command,
                arg.getBadGLEErrorCode(),
                arg.getBadGLEMessage(),
                connection.getConnectionId(),
                writeOpResult,
                arg,
                awaitReplication
        );
    }

}
