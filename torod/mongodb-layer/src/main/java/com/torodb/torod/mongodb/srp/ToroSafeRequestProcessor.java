
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.server.api.SafeRequestProcessor;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.Connection;
import com.eightkdata.mongowp.server.api.CommandsExecutor;
import com.eightkdata.mongowp.server.api.CommandReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.ErrorCode;
import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.messages.utils.IterableDocumentProvider;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.mongodb.MongoLayerConstants;
import com.torodb.torod.mongodb.OptimeClock;
import com.torodb.torod.mongodb.RequestContext;
import com.torodb.torod.mongodb.crp.CollectionRequestProcessor;
import com.torodb.torod.mongodb.crp.CollectionRequestProcessor.QueryResponse;
import com.torodb.torod.mongodb.crp.CollectionRequestProcessorProvider;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import io.netty.util.AttributeMap;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the deepest {@linkplain SafeRequestProcessor} and it is the one that
 * delegates the requests on the {@link Torod} server.
 *
 * It does not check things like the database that is used or if the request
 * has sense on the current state of the replication node. It simply translate
 * the request from MongoDB language to ToroDB language and then executes it.
 */
@Singleton
public class ToroSafeRequestProcessor implements SafeRequestProcessor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroSafeRequestProcessor.class);
	
    private final Torod torod;
    private final String supportedDatabase;
    private final CommandsLibrary commandsLibrary;
    private final CommandsExecutor commandsExecutor;

    private final CollectionRequestProcessorProvider collectionRPProvider;
    
    private final Provider<ReplCoordinator> replCoordinator;
    private final OptimeClock optimeClock;

    @Inject
    public ToroSafeRequestProcessor(
            Torod torod,
            @DatabaseName String supportedDatabase,
            CollectionRequestProcessorProvider collectionRPProvider,
            CommandsLibrary commandsLibrary,
            CommandsExecutor commandsExecutor,
            Provider<ReplCoordinator> replCoordinator,
            OptimeClock optimeClock) {
        this.torod = torod;
        this.supportedDatabase = supportedDatabase;
        this.replCoordinator = replCoordinator;
        this.optimeClock = optimeClock;
        this.commandsLibrary = commandsLibrary;
        this.commandsExecutor = commandsExecutor;
        this.collectionRPProvider = collectionRPProvider;
    }

    @Override
    public void onConnectionActive(Connection connection) {
        ToroConnection toroConnection = torod.openConnection();
        RequestContext context = new RequestContext(
                supportedDatabase,
                toroConnection,
                replCoordinator.get(),
                optimeClock
        );
        context.setTo(connection.getAttributeMap());
    }

    @Override
    public void onConnectionInactive(Connection connection) {
        AttributeMap attMap = connection.getAttributeMap();
        
        RequestContext context = RequestContext.getAndRemoveFrom(attMap);
        
        context.getToroConnection().close();
    }

    /*
     * We don't need to use the default implementation of HubSafeRequestProcessor
     */
    @Override
    public ReplyMessage getMore(Request req, GetMoreMessage getMoreMessage)
            throws MongoException {
		ToroConnection toroConnection = RequestContext.getFrom(req).getToroConnection();

        CursorId cursorId = new CursorId(getMoreMessage.getCursorId());

        try {
            UserCursor cursor = toroConnection.getCursor(cursorId);
            FluentIterable<? extends BsonDocument> results = cursor
                    .read(MongoLayerConstants.MONGO_CURSOR_LIMIT)
                    .transform(ToroToBsonTranslatorFunction.INSTANCE);
            boolean cursorEmptied = results.size() < MongoLayerConstants.MONGO_CURSOR_LIMIT;

            Integer position = cursor.getPosition();
            if (cursorEmptied) {
                cursor.close();
            }

            return new ReplyMessage(
                    EmptyBsonContext.getInstance(),
                    req.getRequestId(),
                    false,
                    false,
                    false,
                    false,
                    cursorEmptied ? 0 : cursorId.getNumericId(),
                    position,
                    IterableDocumentProvider.of(results)
            );
        } catch (CursorNotFoundException | ClosedToroCursorException ex) {
            throw new com.eightkdata.mongowp.exceptions.CursorNotFoundException(cursorId.getNumericId());
        }
    }

    @Override
    public ListenableFuture<?> killCursors(Request req, KillCursorsMessage killCursorsMessage)
            throws MongoException {
		ToroConnection toroConnection = RequestContext.getFrom(req).getToroConnection();

		if (toroConnection == null) {
			throw new MongoException("Unexpected state", ErrorCode.INTERNAL_ERROR);
		}

		int numberOfCursors = killCursorsMessage.getNumberOfCursors();
		long[] cursorIds = killCursorsMessage.getCursorIds();
		for (int index = 0; index < numberOfCursors; index++) {
			CursorId cursorId = new CursorId(cursorIds[index]);

            try {
                toroConnection.getCursor(cursorId).close();
            } catch (CursorNotFoundException ex) {
            }
		}
        return Futures.immediateFuture(null);
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        return commandsLibrary;
    }

    @Override
    public <Arg, Result> CommandReply<Result> execute(Command<? super Arg, ? super Result> command, CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        return commandsExecutor.execute(command, request);
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest message) throws
            MongoException {
        CollectionRequestProcessor colRequestProcessor = collectionRPProvider.getCollectionRequestProcessor(
                request.getDatabase(),
                message.getCollection()
        );
        QueryResponse response = colRequestProcessor.query(request, message);
        return new ReplyMessage(
                EmptyBsonContext.getInstance(),
                request.getRequestId(),
                false,
                false,
                false,
                false,
                response.getCursorId(),
                message.getNumberToSkip(),
                response.getDocuments()
        );
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage message)
            throws MongoException {
        return collectionRPProvider.getCollectionRequestProcessor(
                request.getDatabase(),
                message.getCollection()
        )
                .insert(request, message);
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage message)
            throws MongoException {
        return collectionRPProvider.getCollectionRequestProcessor(
                request.getDatabase(),
                message.getCollection()
        )
                .update(request, message);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage message)
            throws MongoException {
        return collectionRPProvider.getCollectionRequestProcessor(
                request.getDatabase(),
                message.getCollection()
        )
                .delete(request, message);
    }

}
