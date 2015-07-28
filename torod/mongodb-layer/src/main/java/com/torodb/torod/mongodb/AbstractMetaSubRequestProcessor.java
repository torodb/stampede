
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.DeleteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.api.tools.ReplyBuilder;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.bson.BsonDocument;

/**
 *
 */
public abstract class AbstractMetaSubRequestProcessor implements SafeRequestProcessor.SubRequestProcessor {

    private final String collectionName;
    private final String databaseName;
    private final @Nonnull QueryCriteriaTranslator queryCriteriaTranslator;

    public AbstractMetaSubRequestProcessor(
            @Nonnull String collectionName,
            @Nonnull @DatabaseName String databaseName,
            @Nonnull QueryCriteriaTranslator queryCriteriaTranslator) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    protected abstract CommandsExecutor getCommandsExecutor();

    @Override
    public <Arg extends CommandArgument, Rep extends CommandReply> Rep execute(
            Command<? extends Arg, ? extends Rep> command,
            CommandRequest<Arg> request) throws MongoServerException, CommandNotSupportedException {
        return getCommandsExecutor().execute(command, request);
    }

    public abstract List<ToroDocument> queryAllDocuments(Connection connection)
            throws RuntimeException;

    @Override
    public ReplyMessage query(Request req, QueryRequest queryMessage)
            throws MongoServerException {

        int requestId = req.getRequestId();
        if (!queryMessage.getDatabase().equals(databaseName)) {
            return new ReplyMessage(requestId, 0, 0, Collections.<BsonDocument>emptyList());
        
        }
        List<ToroDocument> allDocuments;
        try {
            allDocuments = queryAllDocuments(req.getConnection());
        } catch (RuntimeException ex) {
            return ReplyBuilder.createStandardErrorReplyWithMessage(requestId, ErrorCode.UNKNOWN_ERROR, ex.getLocalizedMessage());
        }
        
        QueryCriteria queryCriteria;
        if (queryMessage.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        } else {
            queryCriteria = queryCriteriaTranslator.translate(queryMessage.getQuery());
        }
        List<ToroDocument> filtrered
                = applyQueryCriteria(allDocuments, queryCriteria);

        List<BsonDocument> result = Lists.transform(
                filtrered,
                ToroToBsonTranslatorFunction.INSTANCE
        );

        return new ReplyMessage(requestId, 0, 0, result);
    }

    @Override
    public Future<? extends WriteOpResult> insert(Request req, InsertMessage insertMessage)
            throws MongoServerException {
        return Futures.immediateFuture(
                new SimpleWriteOpResult(
                        ErrorCode.OPERATION_FAILED,
                        "Insert on " + collectionName + " collection is not supported",
                        null,
                        null
                )
        );
    }

    @Override
    public Future<? extends WriteOpResult> update(Request req, UpdateMessage deleteMessage)
            throws MongoServerException {
        return Futures.immediateFuture(
                new UpdateOpResult(
                        0,
                        false,
                        ErrorCode.OPERATION_FAILED,
                        "Update on sys" + collectionName + " collection is not supported",
                        null,
                        null
                )
        );
    }

    @Override
    public Future<? extends WriteOpResult> delete(Request req, DeleteMessage deleteMessage)
            throws MongoServerException {
        return Futures.immediateFuture(
                new DeleteOpResult(
                        0,
                        ErrorCode.OPERATION_FAILED,
                        "Delete on " + collectionName + " collection is not supported",
                        null,
                        null
                )
        );
    }


    private static class QCEvaluatorPredicate implements Predicate<ToroDocument> {

        private final QueryCriteria qc;

        public QCEvaluatorPredicate(QueryCriteria qc) {
            this.qc = qc;
        }

        @Override
        public boolean apply(ToroDocument input) {
            if (input == null) {
                throw new IllegalArgumentException("The document to filter must be not null");
            }
            return DocValueQueryCriteriaEvaluator.evaluate(qc, input.getRoot());
        }

    }

    private List<ToroDocument> applyQueryCriteria(
            List<ToroDocument> candidates,
            QueryCriteria queryCriteria) {


        QCEvaluatorPredicate predicate = new QCEvaluatorPredicate(queryCriteria);

        return Lists.newArrayList(Iterables.filter(candidates, predicate));
    }

}
