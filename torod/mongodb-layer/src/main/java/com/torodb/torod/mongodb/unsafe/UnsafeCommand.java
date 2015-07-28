
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.Command;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleArgument;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.SimpleReply;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeArgument;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeReply;
import org.bson.BsonDocument;

/**
 *
 */
public class UnsafeCommand implements Command<UnsafeArgument, UnsafeReply> {

    private final QueryCommand queryCommand;

    public UnsafeCommand(QueryCommand queryCommand) {
        this.queryCommand = queryCommand;
    }

    public QueryCommand getQueryCommand() {
        return queryCommand;
    }

    @Override
    public String getCommandName() {
        return queryCommand.getKey();
    }

    @Override
    public boolean isAdminOnly() {
        return queryCommand.isAdminOnly();
    }

    @Override
    public boolean isSlaveOk() {
        return false;
    }

    @Override
    public boolean isSlaveOverrideOk() {
        return true;
    }

    @Override
    public boolean shouldAffectCommandCounter() {
        return true;
    }

    @Override
    public boolean isAllowedOnMaintenance() {
        return false;
    }

    @Override
    public Class<? extends UnsafeArgument> getArgClass() {
        return UnsafeArgument.class;
    }

    @Override
    public UnsafeArgument unmarshallArg(BsonDocument requestDoc) throws
            MongoServerException {
        return new UnsafeArgument(requestDoc, this);
    }

    @Override
    public BsonDocument marshallArg(UnsafeArgument request) throws
            MongoServerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Class<? extends UnsafeReply> getReplyClass() {
        return UnsafeReply.class;
    }

    @Override
    public UnsafeReply unmarshallReply(BsonDocument replyDoc) throws
            MongoServerException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public BsonDocument marshallReply(UnsafeReply reply) throws
            MongoServerException {

        ImmutableList<BsonDocument> documents = reply.getReply().getDocuments();

        if (documents.size() > 1 || documents.isEmpty()) {
            throw new MongoServerException(
                    "Only one document was expected as reply from '"
                    +queryCommand.getKey()+"', but " + documents.size()
                    + " were recived",
                    ErrorCode.INTERNAL_ERROR
            );
        }
        return documents.get(0);
    }


    public static class UnsafeArgument extends SimpleArgument {

        private final BsonDocument argument;

        public UnsafeArgument(BsonDocument argument, Command command) {
            super(command);
            this.argument = argument;
        }

        public BsonDocument getArgument() {
            return argument;
        }
    }

    public static class UnsafeReply extends SimpleReply {
        private final ReplyMessage reply;

        public UnsafeReply(ReplyMessage reply, Command command) {
            super(command);
            this.reply = reply;
        }

        public UnsafeReply(ReplyMessage reply, Command command, ErrorCode errorCode, String errorMessage) {
            super(command, errorCode, errorMessage);
            this.reply = reply;
        }

        public UnsafeReply(ReplyMessage reply, Command command, ErrorCode errorCode, Object... args) {
            super(command, errorCode, args);
            this.reply = reply;
        }

        public ReplyMessage getReply() {
            return reply;
        }

    }
}
