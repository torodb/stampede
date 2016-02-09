
package com.torodb.torod.mongodb.commands.impl.torodb;

import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.InternalErrorException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.server.api.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.server.api.tools.Empty;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.utils.ToroDBThrowables;

/**
 *
 */
public class DropPathViewsImplementation implements CommandImplementation<CollectionCommandArgument, Empty> {

    @Override
    public CommandResult<Empty> apply(
            Command<? super CollectionCommandArgument, ? super Empty> command,
            CommandRequest<CollectionCommandArgument> req) throws MongoException {

        RequestContext context = RequestContext.getFrom(req);
        String supportedDatabase = context.getSupportedDatabase();

        String commandName = command.getCommandName();
        CollectionCommandArgument arg = req.getCommandArgument();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    commandName,
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = context.getToroConnection();
        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
            ToroDBThrowables.getFromCommand(
                    commandName,
                    transaction.dropPathViews(arg.getCollection())
            );
            ToroDBThrowables.getFromCommand(commandName, transaction.commit());

            return new NonWriteCommandResult<>(Empty.getInstance());
        } catch (ImplementationDbException ex) {
            throw new InternalErrorException(command.getCommandName(), ex);
        }

    }



}
