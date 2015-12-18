package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.Connection;
import com.eightkdata.mongowp.mongoserver.api.safe.ErrorHandler;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonDocumentBuilder;
import com.eightkdata.mongowp.mongoserver.api.safe.tools.bson.BsonField;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoException;
import java.util.Collections;
import javax.annotation.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ToroErrorHandler implements ErrorHandler {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(ToroErrorHandler.class);

    @Override
    @Nullable
    public ReplyMessage handleUnexpectedError(
            Connection connection,
            int requestId,
            boolean canReply,
            Throwable error) {
        if (canReply) {
            LOGGER.warn(
                    "An unexpected error was catched",
                    error
            );
            return new ReplyMessage(
                    requestId,
                    0,
                    0,
                    Collections.singletonList(
                            new BsonDocument().append(
                                    "errmsg",
                                    new BsonString("An unexpected error was catched")
                            ).append(
                                    "code",
                                    new BsonInt32(ErrorCode.UNKNOWN_ERROR.getErrorCode())
                            ).append(
                                    "ok",
                                    MongoWP.BSON_KO
                            )
                    )
            );
        }
        else {
            LOGGER.warn(
                    "An error was catched but it cannot be returned to the user",
                    error
            );
            return null;
        }
    }

    private static final BsonField<String> ERRMSG_FIELD = BsonField.create("errmsg");
    private static final BsonField<Integer> CODE_FIELD = BsonField.create("code");
    private static final BsonField<Double> OK_FIELD = BsonField.create("ok");

    @Override
    @Nullable
    public ReplyMessage handleMongodbException(
            Connection connection,
            int requestId,
            boolean canReply,
            MongoException exception) {
        if (canReply) {
            return new ReplyMessage(
                    requestId,
                    0,
                    0,
                    Collections.singletonList(
                            new BsonDocumentBuilder()
                                    .append(ERRMSG_FIELD, exception.getMessage())
                                    .append(CODE_FIELD, exception.getErrorCode().getErrorCode())
                                    .appendUnsafe(OK_FIELD.getFieldName(), MongoWP.BSON_KO)
                                    .build()
                    )
            );
        }
        else {
            LOGGER.warn(
                    "An error was catched but it cannot be returned to the user",
                    exception
            );
            return null;
        }
    }

}
