
package com.torodb.torod.mongodb.standard.commands.general;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertReply;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.UnknownErrorException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.mongodb.WriteConcern;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.WriteError;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.ToroStandardSubRequestProcessor;
import com.torodb.torod.mongodb.translator.BsonToToroTranslatorFunction;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class InsertImplementation implements CommandImplementation<InsertArgument, InsertReply>{

    @Override
    public InsertReply apply(
            Command<? extends InsertArgument, ? extends InsertReply> command,
            CommandRequest<InsertArgument> req) throws MongoServerException {

        InsertArgument arg = req.getCommandArgument();

        String supportedDatabase = ToroStandardSubRequestProcessor.getSupportedDatabase(req);

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = ToroStandardSubRequestProcessor.getConnection(req);
        
        Iterable<ToroDocument> docsToInsert = Iterables.transform(
                arg.getDocuments(),
                BsonToToroTranslatorFunction.INSTANCE
        );

        ToroTransaction transaction;
        try {
            transaction = connection.createTransaction();
        }
        catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex.getLocalizedMessage());
        }

        WriteFailMode writeFailMode = getWriteFailMode(arg.getWriteConcern());

        Future<InsertResponse> insertResponseFuture = transaction.insertDocuments(
                arg.getCollection(),
                docsToInsert,
                writeFailMode
        );

        Future<?> commitResponseFuture = transaction.commit();

        //TODO(gortiz): Check how commit fails interact with the error cases responses
        InsertResponse insertResponse;
        try {
            insertResponse = insertResponseFuture.get();
            commitResponseFuture.get();
        }
        catch (InterruptedException ex) {
            throw new UnknownErrorException(ex.getLocalizedMessage());
        }
        catch (ExecutionException ex) {
            throw new UnknownErrorException(ex.getLocalizedMessage());
        }

        int n = insertResponse.getInsertedDocsCounter();

        if (insertResponse.isSuccess()) {
            //TODO: Fill repl info
            //TODO: Fill shard info
            return new InsertReply(command, n, null, null);
        }
        else {
            return new InsertReply(
                    command,
                    ErrorCode.COMMAND_FAILED,
                    "Something went wrong",
                    n,
                    translateErrors(insertResponse.getErrors()),
                    null,
                    null,
                    null
            );
        }
    }

    private WriteFailMode getWriteFailMode(WriteConcern writeConcern) {
        return WriteFailMode.TRANSACTIONAL;
    }

    private ImmutableList<InsertCommand.InsertReply.WriteError> translateErrors(
            Collection<WriteError> errors) {

        Builder<InsertCommand.InsertReply.WriteError> builder = ImmutableList.builder();

        for (WriteError error : errors) {
            builder.add(
                    new InsertCommand.InsertReply.WriteError(
                            error.getIndex(),
                            error.getCode(),
                            error.getMsg()
                    )
            );
        }

        return builder.build();
    }
}
