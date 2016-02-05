
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.bson.BsonDocument;
import com.eightkdata.mongowp.bson.utils.BsonDocumentReader.AllocationType;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.MarshalException;
import com.eightkdata.mongowp.server.api.deprecated.QueryCommandProcessor.QueryCommand;
import com.google.common.collect.ImmutableList;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeArgument;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class UnsafeCommand implements Command<UnsafeArgument, UnsafeReply> {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(UnsafeCommand.class);
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
        LOGGER.warn("Command {} will be treated as SlaveOk because there is no safe implementation yet", queryCommand.getKey());
        return true;
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
    public boolean canChangeReplicationState() {
        return false;
    }

    @Override
    public Class<? extends UnsafeArgument> getArgClass() {
        return UnsafeArgument.class;
    }

    @Override
    public UnsafeArgument unmarshallArg(BsonDocument requestDoc) {
        return new UnsafeArgument(requestDoc);
    }

    @Override
    public BsonDocument marshallArg(UnsafeArgument request) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Class<? extends UnsafeReply> getResultClass() {
        return UnsafeReply.class;
    }

    @Override
    public UnsafeReply unmarshallResult(BsonDocument replyDoc) throws
            MongoException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public BsonDocument marshallResult(UnsafeReply reply) throws MarshalException {

        ImmutableList<? extends BsonDocument> documents = reply.getReply().getDocuments().getIterable(AllocationType.HEAP).toList();

        if (documents.size() > 1 || documents.isEmpty()) {
            throw new MarshalException(
                    "Only one document was expected as reply from '"
                    +queryCommand.getKey()+"', but " + documents.size()
                    + " were recived"
            );
        }
        return documents.get(0);
    }

    @Override
    public boolean isReadyToReplyResult(UnsafeReply r) {
        return true;
    }


    public static class UnsafeArgument {

        private final BsonDocument argument;

        public UnsafeArgument(BsonDocument argument) {
            this.argument = argument;
        }

        public BsonDocument getArgument() {
            return argument;
        }
    }

    public static class UnsafeReply {
        private final ReplyMessage reply;

        public UnsafeReply(ReplyMessage reply) {
            this.reply = reply;
        }

        public ReplyMessage getReply() {
            return reply;
        }

    }
}
