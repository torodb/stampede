
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.DeleteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleWriteOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.api.tools.ReplyBuilder;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
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
import com.torodb.torod.mongodb.annotations.Standard;
import com.torodb.torod.mongodb.futures.DeleteFuture;
import com.torodb.torod.mongodb.futures.InsertFuture;
import com.torodb.torod.mongodb.futures.UpdateFuture;
import com.torodb.torod.mongodb.translator.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.AttributeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.torodb.torod.mongodb.ToroSafeRequestProcessor.CONNECTION;
import static com.torodb.torod.mongodb.ToroSafeRequestProcessor.SUPPORTED_DATABASE;

/**
 *
 */
public class ToroStandardSubRequestProcessor implements SafeRequestProcessor.SubRequestProcessor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroStandardSubRequestProcessor.class);

    private final QueryCriteriaTranslator queryCriteriaTranslator;
    private final CommandsLibrary commandsLibrary;
    private final CommandsExecutor commandsExecutor;

    @Inject
    public ToroStandardSubRequestProcessor(
            QueryCriteriaTranslator queryCriteriaTranslator,
            @Standard CommandsLibrary commandsLibrary,
            @Standard CommandsExecutor commandsExecutor) {
        this.queryCriteriaTranslator = queryCriteriaTranslator;
        this.commandsLibrary = commandsLibrary;
        this.commandsExecutor = commandsExecutor;
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        return commandsLibrary;
    }

    public static ToroConnection getConnection(AttributeMap attMap) {
        return attMap.attr(CONNECTION).get();
    }

    public static ToroConnection getConnection(Request req) {
        return req.getConnection().getAttributeMap().attr(CONNECTION).get();
    }

    //TODO(gortiz): Right now Torodb only supports a single database, we should remove that in future
    public static String getSupportedDatabase(Request req) {
        return req.getConnection().getAttributeMap().attr(SUPPORTED_DATABASE).get();
    }

    @Override
    @SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    public ReplyMessage query(Request req, QueryRequest request)
            throws MongoServerException {
        ToroConnection toroConnection
                = req.getConnection().getAttributeMap().attr(CONNECTION).get();

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
            return ReplyBuilder.createStandardErrorReplyWithMessage(
                    req.getRequestId(),
                    ErrorCode.UNKNOWN_ERROR,
                    ex.getLocalizedMessage()
            );
        }

        try {
            results = Lists.transform(
                    cursor.read(MongoWP.MONGO_CURSOR_LIMIT),
                    ToroToBsonTranslatorFunction.INSTANCE
            );
        }
        catch (ClosedToroCursorException ex) {
            LOGGER.warn("A newly open cursor was found closed");
            return ReplyBuilder.createStandardErrorReply(
                    req.getRequestId(),
                    ErrorCode.CURSOR_NOT_FOUND,
                    cursor.getId()
            );
        }

        long cursorIdReturned = 0;

        if (results.size() >= MongoWP.MONGO_CURSOR_LIMIT) {
            cursorIdReturned = cursor.getId().getNumericId();
        }
        else {
            cursor.close();
        }

        return new ReplyMessage(req.getRequestId(), cursorIdReturned, 0, results);
    }

    private WriteFailMode getWriteFailMode(InsertMessage message) {
        return WriteFailMode.TRANSACTIONAL;
    }

    @Override
    public Future<? extends WriteOpResult> insert(Request req, InsertMessage insertMessage)
            throws MongoServerException {
        ToroConnection toroConnection = req.getConnection().getAttributeMap().attr(CONNECTION).get();

		String collection = insertMessage.getCollection();
        WriteFailMode writeFailMode = getWriteFailMode(insertMessage);
        List<BsonDocument> documents = insertMessage.getDocuments();
        List<ToroDocument> inserts = Lists.transform(
                documents,
                BsonToToroTranslatorFunction.INSTANCE
        );
        ToroTransaction transaction = null;
        try {
            transaction = toroConnection.createTransaction();

            Future<InsertResponse> futureInsertResponse = transaction.insertDocuments(collection, inserts, writeFailMode);

            Future<?> futureCommitResponse = transaction.commit();

            return new InsertFuture(futureInsertResponse, futureCommitResponse);
        }
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new SimpleWriteOpResult(ErrorCode.UNKNOWN_ERROR, ex.getLocalizedMessage(), null, null)
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
    }

    @Override
    public Future<? extends WriteOpResult> update(Request req, UpdateMessage updateMessage)
            throws MongoServerException {
		ToroConnection toroConnection = req.getConnection().getAttributeMap().attr(CONNECTION).get();

    	String collection = updateMessage.getCollection();
    	WriteFailMode writeFailMode = WriteFailMode.ORDERED;
    	BsonDocument selector = updateMessage.getSelector();
    	for (String key : selector.keySet()) {
    		if (QueryModifier.getByKey(key) != null || QuerySortOrder.getByKey(key) != null) {
                LOGGER.warn("Detected unsuported modifier {}", key);
    			return Futures.immediateFuture(
                        new UpdateOpResult(
                                0,
                                false,
                                ErrorCode.OPERATION_FAILED,
                                "Modifier " + key + " not supported",
                                null,
                                null)
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
	       	Future<UpdateResponse> futureUpdateResponse = transaction.update(collection, updates, writeFailMode);

            Future<?> futureCommitResponse = transaction.commit();

            return new UpdateFuture(
                    futureUpdateResponse,
                    futureCommitResponse
            );
		}
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new UpdateOpResult(
                            0,
                            false,
                            ErrorCode.UNKNOWN_ERROR,
                            ex.getLocalizedMessage(),
                            null,
                            null
                    )
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
    	}
    }

    @Override
    public Future<? extends WriteOpResult> delete(Request req, DeleteMessage deleteMessage)
            throws MongoServerException {
		ToroConnection toroConnection = req.getConnection().getAttributeMap().attr(CONNECTION).get();

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
                                null)
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
	       	Future<DeleteResponse> futureDeleteResponse = transaction.delete(collection, deletes, writeFailMode);

            Future<?> futureCommitResponse = transaction.commit();

            return new DeleteFuture(futureDeleteResponse, futureCommitResponse);
		}
        catch (ImplementationDbException ex) {
            return Futures.immediateFuture(
                    new DeleteOpResult(
                            0,
                            ErrorCode.UNKNOWN_ERROR,
                            ex.getLocalizedMessage(),
                            null,
                            null
                    )
            );
        } finally {
            if (transaction != null) {
                transaction.close();
            }
    	}
    }

    @Override
    public <Arg extends CommandArgument, Rep extends CommandReply> Rep execute(
            Command<? extends Arg, ? extends Rep> command,
            CommandRequest<Arg> request) throws MongoServerException, CommandNotSupportedException {
        return commandsExecutor.execute(command, request);
    }
}
