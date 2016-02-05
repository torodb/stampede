
package com.torodb.torod.mongodb.crp;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.messages.utils.IterableDocumentProvider;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.DeleteOpResult;
import com.eightkdata.mongowp.server.api.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.core.utils.DocValueQueryCriteriaEvaluator;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.meta.MetaCollection;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 *
 */
public class MetaCollectionRequestProcessor implements CollectionRequestProcessor {

    private final MetaCollection metaCollection;
    private final @Nonnull QueryCriteriaTranslator queryCriteriaTranslator;
    private final OptimeClock optimeClock;

    public MetaCollectionRequestProcessor(
            @Nonnull MetaCollection metaCollection,
            @Nonnull QueryCriteriaTranslator queryCriteriaTranslator,
            @Nonnull OptimeClock optimeClock) {
        this.metaCollection = metaCollection;
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.optimeClock = optimeClock;
    }

    @Override
    public QueryResponse query(Request req, QueryRequest queryMessage)
            throws MongoException {

        int requestId = req.getRequestId();
        if (!queryMessage.getDatabase().equals(metaCollection.getDatabaseName())) {
            return new QueryResponse(requestId, IterableDocumentProvider.of(Collections.<BsonDocument>emptyList()));
        }
        List<ToroDocument> allDocuments;
        try {
            ToroConnection toroConnection =
                RequestContext.getFrom(req.getConnection().getAttributeMap())
                .getToroConnection();
            allDocuments = metaCollection.queryAllDocuments(toroConnection);
        } catch (RuntimeException ex) {
            throw new UnknownErrorException(ex);
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

        return new QueryResponse(0, IterableDocumentProvider.of(result));
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request req, InsertMessage insertMessage)
            throws MongoException {
        return Futures.immediateFuture(
                new SimpleWriteOpResult(
                        ErrorCode.OPERATION_FAILED,
                        "Insert on " + metaCollection.getCollectionName() + " collection is not supported",
                        null,
                        null,
                        optimeClock.tick()
                )
        );
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request req, UpdateMessage deleteMessage)
            throws MongoException {
        return Futures.immediateFuture(
                new UpdateOpResult(
                        0,
                        0,
                        false,
                        ErrorCode.OPERATION_FAILED,
                        "Update on sys" + metaCollection.getCollectionName() + " collection is not supported",
                        null,
                        null,
                        optimeClock.tick()
                )
        );
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request req, DeleteMessage deleteMessage)
            throws MongoException {
        return Futures.immediateFuture(
                new DeleteOpResult(
                        0,
                        ErrorCode.OPERATION_FAILED,
                        "Delete on " + metaCollection.getCollectionName() + " collection is not supported",
                        null,
                        null,
                        optimeClock.tick()
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
