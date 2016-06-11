
package com.torodb.torod.mongodb.commands.impl.general;

import java.util.List;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.exceptions.CommandFailed;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpdateStatement;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.UpdateCommand.UpsertResult;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.CommandResult;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.impl.WriteCommandResult;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.kvdocument.conversion.mongowp.MongoWPConverter;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.connection.UpdateResponse;
import com.torodb.torod.core.connection.UpdateResponse.InsertedDocuments;
import com.torodb.torod.core.connection.exceptions.RetryTransactionException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ToroRuntimeException;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.update.SetDocumentUpdateAction;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.language.update.utils.UpdateActionVisitorAdaptor;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.RetryException;
import com.torodb.torod.mongodb.commands.AbstractToroRetryCommandImplementation;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction;
import com.torodb.torod.mongodb.commands.impl.general.update.UpdateActionVisitorDocumentToInsert;
import com.torodb.torod.mongodb.commands.impl.general.update.UpdateActionVisitorDocumentToUpdate;
import com.torodb.torod.mongodb.repl.ObjectIdFactory;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.translator.UpdateActionTranslator;

/**
 *
 */
public class UpdateImplementation extends AbstractToroRetryCommandImplementation<UpdateArgument, UpdateResult>{

    public static final int MAX_RETRY_COUNT = 64;
    
    private final WriteConcernToWriteFailModeFunction toWriteFailModeFunction;
    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final ObjectIdFactory objectIdFactory;
    private final Function<UpdateStatement, UpdateOperation> toUpdateOperation;

    public UpdateImplementation(WriteConcernToWriteFailModeFunction toWriteFailModeFunction, QueryCriteriaTranslator queryCriteriaTranslator, DocumentBuilderFactory documentBuilderFactory, ObjectIdFactory objectIdFactory) {
        this.toWriteFailModeFunction = toWriteFailModeFunction;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.documentBuilderFactory = documentBuilderFactory;
        this.objectIdFactory = objectIdFactory;
        this.toUpdateOperation = new ToUpdateOperationFunction();
    }

    @Override
    public CommandResult<UpdateResult> tryApply(
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

        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
            WriteFailMode writeFailMode = toWriteFailModeFunction.apply(arg.getWriteConcern());

            List<UpdateOperation> updates = Lists.newArrayList(
                    Iterables.transform(arg.getStatements(), toUpdateOperation)
            );
            
            ListenableFuture<UpdateResponse> updateFuture = transaction.update(
                    arg.getCollection(),
                    updates,
                    writeFailMode,
                    new UpdateActionVisitorDocumentToInsert(documentBuilderFactory, objectIdFactory),
                    new UpdateActionVisitorDocumentToUpdate(documentBuilderFactory)
            );
            ListenableFuture<?> commitFuture = transaction.commit();

            UpdateResponse response = Futures.get(updateFuture, UnknownErrorException.class);
            Futures.get(commitFuture, UnknownErrorException.class);
            OpTime optime = context.getOptimeClock().tick();
            UpdateResult updateResult;
            UpdateOpResult writeOpResult;
            ImmutableList.Builder<UpsertResult> upsertResultsBuilder =
                    ImmutableList.builder();
            for (InsertedDocuments insertedDocuments : response.getInsertedDocuments()) {
                upsertResultsBuilder.add(updates.get(insertedDocuments.getOperationIndex())
                        .getAction().accept(new UpdataActionVisitorUpsertResult(
                                insertedDocuments.getOperationIndex(), 
                                insertedDocuments.getDoc()), null));
            }
            if (response.isSuccess()) {
                updateResult = new UpdateResult(
                        response.getModified(),
                        response.getCandidates(),
                        upsertResultsBuilder.build()
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
                        response.getModified(),
                        response.getCandidates(),
                        upsertResultsBuilder.build(),
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
            return new WriteCommandResult<>(updateResult, writeOpResult);
        } catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex);
        }
    }
    
    private static class UpdataActionVisitorUpsertResult extends UpdateActionVisitorAdaptor<UpsertResult, Void> {
        
        private final int operationIndex;
        private final ToroDocument doc;
        
        private UpdataActionVisitorUpsertResult(int operationIndex, ToroDocument doc) {
            super();
            this.operationIndex = operationIndex;
            this.doc = doc;
        }
        
        @Override
        public UpsertResult defaultCase(UpdateAction action, Void arg) {
            throw new ToroRuntimeException();
        }
        @Override
        public UpsertResult visit(SetDocumentUpdateAction action, Void arg) {
            return new UpsertResult(
                    operationIndex, 
                    MongoWPConverter.translate(doc.getRoot().get("_id")));
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
                    !input.isMulti()
            );
        }
    }
}
