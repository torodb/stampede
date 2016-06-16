
package com.torodb.mongodb.commands.impl.general;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.Status;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.commands.general.InsertCommand.InsertResult;
import com.eightkdata.mongowp.mongoserver.api.safe.library.v3m0.pojos.WriteError;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.server.api.impl.WriteCommandResult;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.util.concurrent.Futures;
import com.torodb.mongodb.commands.impl.AbstractTorodbCommandImplementation;
import com.torodb.mongodb.core.MongodConnection;
import com.torodb.mongodb.core.Retrier;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.commands.WriteConcernToWriteFailModeFunction;
import com.torodb.torod.mongodb.translator.BsonToToroTranslatorFunction;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 *
 */
public class InsertImplementation extends AbstractTorodbCommandImplementation<InsertArgument, InsertResult>{

    private final WriteConcernToWriteFailModeFunction toWriteFailModeFunction;

    public InsertImplementation(Retrier retrier, WriteConcernToWriteFailModeFunction toWriteFailModeFunction) {
        super(InsertCommand.INSTANCE.getCommandName(), retrier);
        this.toWriteFailModeFunction = toWriteFailModeFunction;
    }

    @Override
    public Status<InsertResult> apply(Command<? super InsertArgument, ? super InsertResult> command, InsertArgument arg, Request<MongodConnection> req) {
        MongodConnection connection = req.getConnection().getConnectionInfo();
        
        FluentIterable<ToroDocument> docsToInsert = arg.getDocuments().transform(
                BsonToToroTranslatorFunction.INSTANCE
        );

        try (ToroTransaction transaction
                = connection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {

            WriteFailMode writeFailMode
                    = toWriteFailModeFunction.apply(arg.getWriteConcern());

            Future<InsertResponse> insertFuture = transaction.insertDocuments(
                    arg.getCollection(),
                    docsToInsert,
                    writeFailMode
            );

            Future<?> commitFuture = transaction.commit();
            transaction.close();

            //TODO(gortiz): Check how commit fails interact with the error cases responses
            InsertResponse insertResponse = Futures.get(
                    insertFuture, UnknownErrorException.class);
            Futures.get(commitFuture, UnknownErrorException.class);

            int n = insertResponse.getInsertedDocsCounter();

            InsertResult result;
            WriteOpResult writeOpResult;
            OpTime optime = context.getOptimeClock().tick();
            if (insertResponse.isSuccess()) {
                //TODO: Fill repl info
                //TODO: Fill shard info
                result = new InsertResult(n);
                writeOpResult
                        = new SimpleWriteOpResult(ErrorCode.OK, null, null, null, optime);
            } else {
                ErrorCode errorCode = ErrorCode.COMMAND_FAILED;
                String errMsg = "Something went wrong";
                result = new InsertResult(
                        errorCode,
                        errMsg,
                        n,
                        translateErrors(insertResponse.getErrors()),
                        null
                );
                writeOpResult
                        = new SimpleWriteOpResult(errorCode, errMsg, null, null, optime);
            }
            return new WriteCommandResult<>(result, writeOpResult);
        } catch (ImplementationDbException ex) {
            throw new UnknownErrorException(ex);
        }
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
