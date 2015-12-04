
package com.torodb.torod.mongodb.commands.impl.torodb;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.CollectionCommandArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.NonWriteCommandResult;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.mongodb.RequestContext;

/**
 *
 */
public class CreatePathViewsImplementation implements CommandImplementation<CollectionCommandArgument, Integer> {

    @Override
    public CommandResult<Integer> apply(
            Command<? super CollectionCommandArgument, ? super Integer> command,
            CommandRequest<CollectionCommandArgument> req) throws MongoException {

        RequestContext context = RequestContext.getFrom(req);
        String supportedDatabase = context.getSupportedDatabase();

        CollectionCommandArgument arg = req.getCommandArgument();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = context.getToroConnection();
        ToroTransaction transaction = null;

        try {
            transaction = connection.createTransaction();
            return new NonWriteCommandResult<Integer>(
                    Futures.get(transaction.createPathViews(arg.getCollection()), CommandFailed.class)
            );
        } catch (UnsupportedOperationException ex) {
            throw new CommandFailed(command.getCommandName(), ex.getLocalizedMessage(), ex);
        } catch (ImplementationDbException ex) {
            throw new CommandFailed(command.getCommandName(), ex.getLocalizedMessage(), ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

    }



}
