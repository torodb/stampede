
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.Connection;
import com.eightkdata.mongowp.mongoserver.api.safe.Request;
import com.eightkdata.mongowp.mongoserver.api.safe.SafeRequestProcessor;
import com.eightkdata.mongowp.mongoserver.api.tools.ReplyBuilder;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.torodb.torod.core.Torod;
import com.torodb.torod.core.annotations.DatabaseName;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.core.cursors.CursorId;
import com.torodb.torod.core.cursors.UserCursor;
import com.torodb.torod.core.exceptions.ClosedToroCursorException;
import com.torodb.torod.core.exceptions.CursorNotFoundException;
import com.torodb.torod.mongodb.annotations.Index;
import com.torodb.torod.mongodb.annotations.Namespaces;
import com.torodb.torod.mongodb.annotations.Standard;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import io.netty.util.AttributeMap;
import java.util.List;
import java.util.concurrent.Future;
import javax.inject.Inject;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class ToroSafeRequestProcessor implements SafeRequestProcessor {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroSafeRequestProcessor.class);
	
    private final Torod torod;
    private final String supportedDatabase;
    private final SubRequestProcessor standardRP;
    private final SubRequestProcessor indexRP;
    private final SubRequestProcessor namespacesRP;

    private final ReplCoordinator replCoordinator;
    private final OptimeClock optimeClock;

    @Inject
    public ToroSafeRequestProcessor(
            Torod torod,
            @DatabaseName String supportedDatabase,
            @Standard SubRequestProcessor standardRP,
            @Index SubRequestProcessor indexRP,
            @Namespaces SubRequestProcessor namespacesRP,
            ReplCoordinator replCoordinator,
            OptimeClock optimeClock) {
        this.torod = torod;
        this.standardRP = standardRP;
        this.indexRP = indexRP;
        this.namespacesRP = namespacesRP;
        this.supportedDatabase = supportedDatabase;
        this.replCoordinator = replCoordinator;
        this.optimeClock = optimeClock;
    }

    @Override
    public void onConnectionActive(Connection connection) {
        ToroConnection toroConnection = torod.openConnection();
        RequestContext context = new RequestContext(
                supportedDatabase,
                toroConnection,
                replCoordinator,
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

    @Override
    public SubRequestProcessor getStantardRequestProcessor() {
        return standardRP;
    }

    @Override
    public SubRequestProcessor getNamespacesRequestProcessor() {
        return namespacesRP;
    }

    @Override
    public SubRequestProcessor getIndexRequestProcessor() {
        return indexRP;
    }

    @Override
    public SubRequestProcessor getJSProcessor() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public SubRequestProcessor getProfileRequestProcessor() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public ReplyMessage getMore(Request req, GetMoreMessage getMoreMessage)
            throws MongoServerException {
		ToroConnection toroConnection = RequestContext.getFrom(req).getToroConnection();

        CursorId cursorId = new CursorId(getMoreMessage.getCursorId());

        try {
            UserCursor cursor = toroConnection.getCursor(cursorId);
            List<BsonDocument> results = Lists.transform(
                    cursor.read(MongoWP.MONGO_CURSOR_LIMIT),
                    ToroToBsonTranslatorFunction.INSTANCE
            );
            boolean cursorEmptied = results.size() < MongoWP.MONGO_CURSOR_LIMIT;

            Integer position = cursor.getPosition();
            if (cursorEmptied) {
                cursor.close();
            }

            return new ReplyMessage(
                    req.getRequestId(),
                    cursorEmptied ? 0 : cursorId.getNumericId(),
                    position,
                    results
            );
        } catch (CursorNotFoundException ex) {
            return ReplyBuilder.createStandardErrorReply(req.getRequestId(), ErrorCode.CURSOR_NOT_FOUND, cursorId);
        } catch (ClosedToroCursorException ex) {
            return ReplyBuilder.createStandardErrorReply(req.getRequestId(), ErrorCode.CURSOR_NOT_FOUND, cursorId);
        }
    }

    @Override
    public Future<?> killCursors(Request req, KillCursorsMessage killCursorsMessage)
            throws MongoServerException {
		ToroConnection toroConnection = RequestContext.getFrom(req).getToroConnection();

		if (toroConnection == null) {
			throw new MongoServerException("Unexpected state", ErrorCode.INTERNAL_ERROR);
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

}
