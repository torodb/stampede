
package com.torodb.torod.mongodb.commands.impl.general;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.WriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.WriteError;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.UnknownErrorException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction;
import com.torodb.torod.mongodb.translator.BsonToToroTranslatorFunction;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.inject.Inject;

/**
 *
 */
public class InsertImplementation implements CommandImplementation<InsertArgument, InsertResult>{

    private final WriteConcernToWriteFailModeFunction toWriteFailModeFunction;

    @Inject
    public InsertImplementation(WriteConcernToWriteFailModeFunction toWriteFailModeFunction) {
        this.toWriteFailModeFunction = toWriteFailModeFunction;
    }


    @Override
    public CommandResult<InsertResult> apply(
            Command<? super InsertArgument, ? super InsertResult> command,
            CommandRequest<InsertArgument> req) throws MongoException {

        InsertArgument arg = req.getCommandArgument();

        RequestContext context = RequestContext.getFrom(req);
        String supportedDatabase = context.getSupportedDatabase();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = context.getToroConnection();
        
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

        WriteFailMode writeFailMode = toWriteFailModeFunction.apply(arg.getWriteConcern());

        Future<InsertResponse> insertResponseFuture = transaction.insertDocuments(
                arg.getCollection(),
                docsToInsert,
                writeFailMode
        );

        Future<?> commitResponseFuture = transaction.commit();
        transaction.close();

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

        InsertResult result;
        WriteOpResult writeOpResult;
        OpTime optime = context.getOptimeClock().tick();
        if (insertResponse.isSuccess()) {
            //TODO: Fill repl info
            //TODO: Fill shard info
            result = new InsertResult(n);
            writeOpResult = new SimpleWriteOpResult(ErrorCode.OK, null, null, null, optime);
        }
        else {
            ErrorCode errorCode = ErrorCode.COMMAND_FAILED;
            String errMsg = "Something went wrong";
            result = new InsertResult(
                    errorCode,
                    errMsg,
                    n,
                    translateErrors(insertResponse.getErrors()),
                    null
            );
            writeOpResult = new SimpleWriteOpResult(errorCode, errMsg, null, null, optime);
        }
        return new WriteCommandResult<InsertResult>(result, writeOpResult);
    }

    private ImmutableList<WriteError> translateErrors(
            Collection<com.torodb.torod.core.connection.WriteError> errors) {

        Builder<WriteError> builder = ImmutableList.builder();

        for (com.torodb.torod.core.connection.WriteError error : errors) {
            builder.add(
                    new WriteError(
                            error.getIndex(),
                            error.getCode(),
                            error.getMsg()
                    )
            );
        }

        return builder.build();
    }
}
