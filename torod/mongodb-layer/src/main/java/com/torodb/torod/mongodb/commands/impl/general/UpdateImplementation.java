
package com.torodb.torod.mongodb.commands.impl.general;

import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.WriteCommandResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateStatement;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandFailed;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.UnknownErrorException;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.UpdateResponse;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.translator.UpdateActionTranslator;
import java.util.List;

/**
 *
 */
public class UpdateImplementation implements CommandImplementation<UpdateArgument, UpdateResult>{

    private final WriteConcernToWriteFailModeFunction toWriteFailModeFunction;
    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final Function<UpdateStatement, UpdateOperation> toUpdateOperation;

    public UpdateImplementation(WriteConcernToWriteFailModeFunction toWriteFailModeFunction, QueryCriteriaTranslator queryCriteriaTranslator) {
        this.toWriteFailModeFunction = toWriteFailModeFunction;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.toUpdateOperation = new ToUpdateOperationFunction();
    }

    @Override
    public CommandResult<UpdateResult> apply(
            Command<? super UpdateArgument, ? super UpdateResult> command,
            CommandRequest<UpdateArgument> req) throws MongoException {

        UpdateArgument arg = req.getCommandArgument();

        RequestContext context = RequestContext.getFrom(req);
        String supportedDatabase = context.getSupportedDatabase();

        if (!supportedDatabase.equals(req.getDatabase())) {
            throw new CommandFailed(
                    command.getCommandName(),
                    "Database '"+req.getDatabase()+"' is not supported. "
                            + "Only '" + supportedDatabase +"' is supported");
        }

        ToroConnection connection = context.getToroConnection();

        ToroTransaction transaction;
        try {
            transaction = connection.createTransaction();
        } catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex.getLocalizedMessage());
        }

        try {
            WriteFailMode writeFailMode = toWriteFailModeFunction.apply(arg.getWriteConcern());

            List<UpdateOperation> updates = Lists.newArrayList(
                    Iterables.transform(arg.getStatements(), toUpdateOperation)
            );
            
            ListenableFuture<UpdateResponse> updateFuture = transaction.update(
                    arg.getCollection(),
                    updates,
                    writeFailMode
            );
            ListenableFuture<?> commitFuture = transaction.commit();

            UpdateResponse response = Futures.get(updateFuture, UnknownErrorException.class);
            Futures.get(commitFuture, UnknownErrorException.class);
            OpTime optime = context.getOptimeClock().tick();
            UpdateResult updateResult;
            UpdateOpResult writeOpResult;
            //TODO: add upsert!
            if (response.isSuccess()) {
                updateResult = new UpdateResult(
                        response.getCandidates(),
                        response.getModified()
                );
                writeOpResult = new UpdateOpResult(
                        response.getCandidates(),
                        response.getModified(),
                        false,
                        ErrorCode.OK,
                        null,
                        null,
                        optime
                );
            } else {
                ErrorCode errorCode = ErrorCode.COMMAND_FAILED;
                String errMsg = "Something went wrong";
                //TODO: translate write errors and write concern errors
                updateResult = new UpdateResult(
                        response.getCandidates(),
                        response.getModified(),
                        errMsg,
                        null,
                        null
                );
                writeOpResult = new UpdateOpResult(
                        response.getCandidates(),
                        response.getModified(),
                        false,
                        errorCode,
                        errMsg,
                        null,
                        null,
                        optime
                );
            }
            return new WriteCommandResult<UpdateResult>(updateResult, writeOpResult);
        } finally {
            transaction.close();
        }
    }

    private class ToUpdateOperationFunction implements Function<UpdateStatement, UpdateOperation> {

            @Override
        public UpdateOperation apply(UpdateStatement input) {
            if (input == null) {
                return null;
            }

            UpdateAction updateAction = UpdateActionTranslator.translate(
    				input.getUpdate()
            );
    		return new UpdateOperation(
                    queryCriteriaTranslator.translate(input.getQuery()),
                    updateAction,
                    input.isUpsert(),
                    input.isMulti()
            );
        }
    }
}
