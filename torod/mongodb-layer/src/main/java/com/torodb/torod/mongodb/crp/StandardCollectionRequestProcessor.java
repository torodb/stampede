
package com.torodb.torod.mongodb.crp;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.OpTime;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.BsonDocument.Entry;
import com.eightkdata.mongowp.bson.BsonValue;
import com.eightkdata.mongowp.bson.utils.BsonDocumentReader.AllocationType;
import com.eightkdata.mongowp.exceptions.CursorNotFoundException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.exceptions.UnknownErrorException;
import com.eightkdata.mongowp.messages.request.DeleteMessage;
import com.eightkdata.mongowp.messages.request.InsertMessage;
import com.eightkdata.mongowp.messages.request.UpdateMessage;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.server.api.impl.DeleteOpResult;
import com.eightkdata.mongowp.server.api.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.WriteFailMode;
import com.torodb.torod.core.config.DocumentBuilderFactory;
import com.torodb.torod.core.connection.DeleteResponse;
import com.torodb.torod.core.connection.InsertResponse;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.connection.ToroTransaction;
import com.torodb.torod.core.connection.TransactionMetainfo;
import com.torodb.torod.core.connection.UpdateResponse;
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
import com.torodb.torod.mongodb.MongoLayerConstants;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.commands.impl.general.update.UpdateActionVisitorDocumentToInsert;
import com.torodb.torod.mongodb.commands.impl.general.update.UpdateActionVisitorDocumentToUpdate;
import com.torodb.torod.mongodb.futures.DeleteFuture;
import com.torodb.torod.mongodb.futures.InsertFuture;
import com.torodb.torod.mongodb.futures.UpdateFuture;
import com.torodb.torod.mongodb.repl.ObjectIdFactory;
import com.torodb.torod.mongodb.translator.BsonToToroTranslatorFunction;
import com.torodb.torod.mongodb.translator.QueryCriteriaTranslator;
import com.torodb.torod.mongodb.translator.QueryEncapsulation;
import com.torodb.torod.mongodb.translator.QueryModifier;
import com.torodb.torod.mongodb.translator.QuerySortOrder;
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import com.torodb.torod.mongodb.translator.UpdateActionTranslator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 */
public class StandardCollectionRequestProcessor implements CollectionRequestProcessor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(StandardCollectionRequestProcessor.class);

    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final OptimeClock optimeClock;
    private final DocumentBuilderFactory documentBuilderFactory;
    private final ObjectIdFactory objectIdFactory;

    @Inject
    public StandardCollectionRequestProcessor(
            QueryCriteriaTranslator queryCriteriaTranslator,
            OptimeClock optimeClock, 
            DocumentBuilderFactory documentBuilderFactory, 
            ObjectIdFactory objectIdFactory) {
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.optimeClock = optimeClock;
        this.documentBuilderFactory = documentBuilderFactory;
        this.objectIdFactory = objectIdFactory;
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

        UserCursor cursor;
        FluentIterable<BsonDocument> results;

        if (request.isTailable()) {
            throw new UserToroException("TailableCursors are not supported");
        }

        boolean autoclose = request.isAutoclose() || !request.isTailable();
        try {
            if (request.getLimit() == 0) {
                cursor = toroConnection.openUnlimitedCursor(
                        request.getCollection(),
                        queryCriteria,
                        projection,
                        request.getNumberToSkip(),
                        autoclose,
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
                        autoclose,
                        !request.isNoCursorTimeout()
                );
            }
        } catch (ToroException ex) {
            throw new UnknownErrorException(ex);
        }

        try {
            results = cursor.read(MongoLayerConstants.MONGO_CURSOR_LIMIT)
                    .transform(ToroToBsonTranslatorFunction.INSTANCE);
        }
        catch (ClosedToroCursorException ex) {
            LOGGER.warn("A newly open cursor was found closed");
            throw new CursorNotFoundException(cursor.getId().getNumericId());
        }

        long cursorIdReturned = 0;

        if (!cursor.isClosed()) {
            cursorIdReturned = cursor.getId().getNumericId();
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
        //TODO: Improve it to use offheap values, which implies to retain the bytebuf
        FluentIterable<ToroDocument> documents = insertMessage.getDocuments()
                .getIterable(AllocationType.HEAP)
                .transform(BsonToToroTranslatorFunction.INSTANCE);
        OpTime optime = optimeClock.tick();

        try (ToroTransaction transaction
                = toroConnection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {

            ListenableFuture<InsertResponse> futureInsertResponse = transaction.insertDocuments(
                    collection,
                    documents,
                    writeFailMode
            );

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new InsertFuture(optime, futureInsertResponse, futureCommitResponse);
        }
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new SimpleWriteOpResult(ErrorCode.UNKNOWN_ERROR, ex.getLocalizedMessage(), null, null, optime)
            );
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

    	for (Entry<?> entry : selector) {
            String key = entry.getKey();
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
    	for (Entry<?> entry : query) {
            String key = entry.getKey();
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
    	boolean upsert = updateMessage.isUpsert();
    	boolean justOne = !updateMessage.isMultiUpdate();
    
        UpdateAction updateAction = UpdateActionTranslator.translate(
                    updateMessage.getUpdate());
    
        updates.add(new UpdateOperation(queryCriteria, updateAction, upsert, justOne));

        try (ToroTransaction transaction
                = toroConnection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
            ListenableFuture<UpdateResponse> futureUpdateResponse = transaction.update(
                    collection, 
                    updates, 
                    writeFailMode,
                    new UpdateActionVisitorDocumentToInsert(documentBuilderFactory, objectIdFactory),
                    new UpdateActionVisitorDocumentToUpdate(documentBuilderFactory));

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new UpdateFuture(
                    optimeClock.tick(),
                    futureUpdateResponse,
                    futureCommitResponse
            );
        } catch (ImplementationDbException ex) {
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
    	for (Entry entry : document) {
            String key = entry.getKey();
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
    	for (Entry entry : query) {
            String key = entry.getKey();
    		if (QueryEncapsulation.getByKey(key) != null) {
    			BsonValue queryObject = query.get(key);
    			if (queryObject != null && queryObject.isDocument()) {
    				query = queryObject.asDocument();
    				break;
    			}
    		}
    	}
    	QueryCriteria queryCriteria = queryCriteriaTranslator.translate(query);
    	List<DeleteOperation> deletes = new ArrayList<>();
    	boolean singleRemove = deleteMessage.isSingleRemove();

    	deletes.add(new DeleteOperation(queryCriteria, singleRemove));

        try (ToroTransaction transaction
                = toroConnection.createTransaction(TransactionMetainfo.NOT_READ_ONLY)) {
	       	ListenableFuture<DeleteResponse> futureDeleteResponse = transaction.delete(collection, deletes, writeFailMode);

            ListenableFuture<?> futureCommitResponse = transaction.commit();

            return new DeleteFuture(
                    optimeClock.tick(),
                    futureDeleteResponse,
                    futureCommitResponse
            );
		} catch (ImplementationDbException ex) {
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
        }
    }
}
