
package com.torodb.torod.mongodb.crp;

import com.torodb.torod.mongodb.crp.CollectionRequestProcessor;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.Request;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.DeleteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.pojos.OpTime;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CursorNotFoundException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.UnknownErrorException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.connection.*;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.ToroException;
import com.torodb.torod.core.exceptions.UserToroException;
import com.torodb.torod.core.language.operations.DeleteOperation;
import com.torodb.torod.core.language.operations.UpdateOperation;
import com.torodb.torod.core.language.projection.Projection;
import com.torodb.torod.core.language.querycriteria.QueryCriteria;
import com.torodb.torod.core.language.querycriteria.TrueQueryCriteria;
import com.torodb.torod.core.language.update.UpdateAction;
import com.torodb.torod.core.subdocument.ToroDocument;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.futures.DeleteFuture;
import com.torodb.torod.mongodb.futures.InsertFuture;
import com.torodb.torod.mongodb.futures.UpdateFuture;
import com.torodb.torod.mongodb.translator.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StandardCollectionRequestProcessor implements CollectionRequestProcessor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(StandardCollectionRequestProcessor.class);

    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final OptimeClock optimeClock;

    @Inject
    public StandardCollectionRequestProcessor(
            QueryCriteriaTranslator queryCriteriaTranslator,
            OptimeClock optimeClock) {
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.optimeClock = optimeClock;
    }

    @Override
    @SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    public QueryResponse query(Request req, QueryRequest request)
            throws MongoException {
        ToroConnection toroConnection = RequestContext.getFrom(req)
                .getToroConnection();

        if (request.getProjection() != null) {
            throw new UserToroException("Projections are not supported");
        }
        QueryCriteria queryCriteria;
        if (request.getQuery() == null) {
            queryCriteria = TrueQueryCriteria.getInstance();
        }
        else {
            queryCriteria = queryCriteriaTranslator.translate(
                    request.getQuery()
            );
        }
        Projection projection = null;

        UserCursor<ToroDocument> cursor;
        List<BsonDocument> results;

        if (request.isTailable()) {
            throw new UserToroException("TailableCursors are not supported");
        }

        try {
            if (request.getLimit() == 0) {
                cursor = toroConnection.openUnlimitedCursor(
                        request.getCollection(),
                        queryCriteria,
                        projection,
                        request.getNumberToSkip(),
                        request.isAutoclose(),
                        !request.isNoCursorTimeout()
                );
            }
            else {
                cursor = toroConnection.openLimitedCursor(
                        request.getCollection(),
                        queryCriteria,
                        projection,
                        request.getNumberToSkip(),
                        request.getLimit(),
                        request.isAutoclose(),
                        !request.isNoCursorTimeout()
                );
            }
        } catch (ToroException ex) {
            throw new UnknownErrorException(ex);
        }

        try {
            results = Lists.transform(
                    cursor.read(MongoWP.MONGO_CURSOR_LIMIT),
                    ToroToBsonTranslatorFunction.INSTANCE
            );
        }
        catch (ClosedToroCursorException ex) {
            LOGGER.warn("A newly open cursor was found closed");
            throw new CursorNotFoundException(cursor.getId().getNumericId());
        }

        long cursorIdReturned = 0;

        if (results.size() >= MongoWP.MONGO_CURSOR_LIMIT) {
            cursorIdReturned = cursor.getId().getNumericId();
        }
        else {
            cursor.close();
        }

        return new QueryResponse(cursorIdReturned, results);
    }

    private WriteFailMode getWriteFailMode(InsertMessage message) {
        return WriteFailMode.TRANSACTIONAL;
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request req, InsertMessage insertMessage)
            throws MongoException {
        ToroConnection toroConnection = RequestContext.getFrom(req)
                .getToroConnection();

		String collection = insertMessage.getCollection();
        WriteFailMode writeFailMode = getWriteFailMode(insertMessage);
        List<? extends BsonDocument> documents = insertMessage.getDocuments();
        List<ToroDocument> inserts = Lists.transform(
                documents,
                BsonToToroTranslatorFunction.INSTANCE
        );
        ToroTransaction transaction = null;

        OpTime optime = optimeClock.tick();

        try {
            transaction = toroConnection.createTransaction();

            ListenableFuture<InsertResponse> futureInsertResponse = transaction.insertDocuments(collection, inserts, writeFailMode);

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new InsertFuture(optime, futureInsertResponse, futureCommitResponse);
        }
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new SimpleWriteOpResult(ErrorCode.UNKNOWN_ERROR, ex.getLocalizedMessage(), null, null, optime)
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request req, UpdateMessage updateMessage)
            throws MongoException {
		ToroConnection toroConnection = RequestContext.getFrom(req)
                .getToroConnection();

    	String collection = updateMessage.getCollection();
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
    	BsonDocument selector = updateMessage.getSelector();

    	for (String key : selector.keySet()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
                LOGGER.warn("Detected unsuported modifier {}", key);
    			return Futures.immediateFuture(
                        new UpdateOpResult(
                                0,
                                0,
                                false,
                                ErrorCode.OPERATION_FAILED,
                                "Modifier " + key + " not supported",
                                null,
                                null,
                                optimeClock.tick()
                        )
                );
    		}
    	}
    	BsonDocument query = selector;
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
                BsonValue queryObject = query.get(key);
    			if (queryObject != null && query.isDocument()) {
    				query = queryObject.asDocument();
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	List<UpdateOperation> updates = Lists.newArrayList();
    	boolean upsert = updateMessage.isFlagSet(UpdateMessage.Flag.UPSERT);
    	boolean justOne = !updateMessage.isFlagSet(UpdateMessage.Flag.MULTI_UPDATE);

    	UpdateAction updateAction = UpdateActionTranslator.translate(
                updateMessage.getUpdate());

    	updates.add(new UpdateOperation(queryCriteria, updateAction, upsert, justOne));

    	ToroTransaction transaction = null;

        try {
            transaction = toroConnection.createTransaction();
	       	ListenableFuture<UpdateResponse> futureUpdateResponse = transaction.update(collection, updates, writeFailMode);

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new UpdateFuture(
                    optimeClock.tick(),
                    futureUpdateResponse,
                    futureCommitResponse
            );
		}
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new UpdateOpResult(
                            0,
                            0,
                            false,
                            ErrorCode.UNKNOWN_ERROR,
                            ex.getLocalizedMessage(),
                            null,
                            null,
                            optimeClock.tick()
                    )
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
    	}
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request req, DeleteMessage deleteMessage)
            throws MongoException {
		ToroConnection toroConnection = RequestContext.getFrom(req)
                .getToroConnection();

    	String collection = deleteMessage.getCollection();
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
    	BsonDocument document = deleteMessage.getDocument();
    	for (String key : document.keySet()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
    			LOGGER.warn("Detected unsuported modifier {}", key);
    			return Futures.immediateFuture(
                        new DeleteOpResult(
                                0,
                                ErrorCode.OPERATION_FAILED,
                                "Modifier " + key + " not supported",
                                null,
                                null,
                                optimeClock.tick()
                        )
                );
    		}
    	}
    	BsonDocument query = document;
    	for (String key : query.keySet()) {
    		if (QueryEncapsulation.getByKey(key) != null) {
    			BsonValue queryObject = query.get(key);
    			if (queryObject != null && queryObject.isDocument()) {
    				query = queryObject.asDocument();
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	List<DeleteOperation> deletes = new ArrayList<DeleteOperation>();
    	boolean singleRemove = deleteMessage.isFlagSet(DeleteMessage.Flag.SINGLE_REMOVE);

    	deletes.add(new DeleteOperation(queryCriteria, singleRemove));

    	ToroTransaction transaction = null;

        try {
            transaction = toroConnection.createTransaction();
	       	ListenableFuture<DeleteResponse> futureDeleteResponse = transaction.delete(collection, deletes, writeFailMode);

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new DeleteFuture(
                    optimeClock.tick(),
                    futureDeleteResponse,
                    futureCommitResponse
            );
		}
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new DeleteOpResult(
                            0,
                            ErrorCode.UNKNOWN_ERROR,
                            ex.getLocalizedMessage(),
                            null,
                            null,
                            optimeClock.tick()
                    )
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
    	}
    }
}
