
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
import com.torodb.torod.mongodb.translator.ToroToBsonTranslatorFunction;
import io.netty.util.AttributeKey;
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
    public final static AttributeKey<ToroConnection> CONNECTION =
			AttributeKey.valueOf("connection");
    public final static AttributeKey<String> SUPPORTED_DATABASE =
			AttributeKey.valueOf("supported-database");
	
    private final Torod torod;
    private final String supportedDatabase;
    private final SubRequestProcessor standardRP;
    private final SubRequestProcessor indexRP;
    private final SubRequestProcessor namespacesRP;

    @Inject
    public ToroSafeRequestProcessor(
            Torod torod,
            @DatabaseName String supportedDatabase,
            @Standard SubRequestProcessor standardRP,
            @Index SubRequestProcessor indexRP,
            @Namespaces SubRequestProcessor namespacesRP) {
        this.torod = torod;
        this.standardRP = standardRP;
        this.indexRP = indexRP;
        this.namespacesRP = namespacesRP;
        this.supportedDatabase = supportedDatabase;
    }

    @Override
    public void onConnectionActive(Connection connection) {
        ToroConnection toroConnection = torod.openConnection();
        AttributeMap attributeMap = connection.getAttributeMap();
        attributeMap.attr(CONNECTION).set(toroConnection);
        attributeMap.attr(SUPPORTED_DATABASE).set(supportedDatabase);
    }

    @Override
    public void onConnectionInactive(Connection connection) {
		ToroConnection toroConnection = connection
                .getAttributeMap()
                .attr(CONNECTION)
                .getAndRemove();
		if (toroConnection != null) {
			toroConnection.close();
		}
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
		ToroConnection toroConnection = req.getConnection().getAttributeMap().attr(CONNECTION).get();

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
		ToroConnection toroConnection = req.getConnection().getAttributeMap().attr(CONNECTION).get();

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
