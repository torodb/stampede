
package com.torodb.torod.mongodb.unsafe;

import com.eightkdata.mongowp.messages.request.RequestBaseMessage;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.MetaCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor;
import com.eightkdata.mongowp.mongoserver.api.QueryCommandProcessor.QueryCommand;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.callback.MessageReplier;
import com.eightkdata.mongowp.mongoserver.protocol.MongoWP.ErrorCode;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.MongoServerException;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeArgument;
import com.torodb.torod.mongodb.unsafe.UnsafeCommand.UnsafeReply;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.AttributeMap;
import javax.inject.Inject;

/**
 *
 */
public class UnsafeCommandsExecutorAdaptor implements CommandsExecutor {

    private final QueryCommandProcessor queryCommandProcessor;
    private final MetaCommandProcessor metaCommandProcessor;

    @Inject
    public UnsafeCommandsExecutorAdaptor(QueryCommandProcessor queryCommandProcessor) {
        this.queryCommandProcessor = queryCommandProcessor;
        metaCommandProcessor = new UnsafeMetaCommandProcessor();
    }

    @SuppressWarnings("cast") //http://bugs.java.com/view_bug.do?bug_id=6548436
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
    @Override
    public <Arg extends CommandArgument, Rep extends CommandReply> Rep execute(
            Command<? extends Arg, ? extends Rep> command,
            CommandRequest<Arg> request)
            throws MongoServerException, CommandNotSupportedException {

//        if (!(command instanceof UnsafeCommand)) { //There is a bug on jdk 6: //http://bugs.java.com/view_bug.do?bug_id=6548436
        if (!UnsafeCommand.class.isAssignableFrom(command.getClass())) {
            throw new IllegalArgumentException("Only commands whose class is "
                    + UnsafeCommand.class.getCanonicalName() + " are supported "
                    + "by this executor");
        }
//        if (!(request.getCommandArgument() instanceof UnsafeArgument)) { //There is a bug on jdk 6: //http://bugs.java.com/view_bug.do?bug_id=6548436
        if (!UnsafeArgument.class.isAssignableFrom(request.getCommandArgument().getClass())) {
            throw new IllegalArgumentException("Only arguments whose class is "
                    + UnsafeArgument.class.getCanonicalName() + " are supported "
                    + "by this executor");
        }

//        UnsafeCommand unsafeCommand = (UnsafeCommand) command; //There is a bug on jdk 6: //http://bugs.java.com/view_bug.do?bug_id=6548436
//        UnsafeArgument unsafeArgument = (UnsafeArgument) request.getCommandArgument(); //There is a bug on jdk 6: //http://bugs.java.com/view_bug.do?bug_id=6548436

        UnsafeCommand unsafeCommand = UnsafeCommand.class.cast(command);
        UnsafeArgument unsafeArgument = UnsafeArgument.class.cast(request.getCommandArgument());

        QueryCommand queryCommand = unsafeCommand.getQueryCommand();

        RequestBaseMessage requestBaseMessage = new RequestBaseMessage(
                request.getClientAddress(),
                request.getClientPort(),
                request.getRequestId()
        );

        FakeMessageReplier fakeMessageReplier = new FakeMessageReplier(
                request.getRequestId(),
                request.getConnection().getConnectionId(),
                request.getConnection().getAttributeMap()
        );

        try {
            queryCommand.call(
                    requestBaseMessage,
                    unsafeArgument.getArgument(),
                    new QueryCommandProcessor.ProcessorCaller(
                            request.getDatabase(),
                            queryCommandProcessor,
                            metaCommandProcessor,
                            fakeMessageReplier
                    )
            );

            ReplyMessage reply = fakeMessageReplier.getReply();

            return (Rep) new UnsafeReply(reply, command);
        }
        catch (Exception ex) {
            throw new MongoServerException(ex.getLocalizedMessage(), ErrorCode.INTERNAL_ERROR);
        }
    }


    private static class FakeMessageReplier extends MessageReplier {

        private final int requestId;
        private final int connectionId;
        private final AttributeMap attributeMap;
        private ReplyMessage reply;

        public FakeMessageReplier(int requestId, int connectionId, AttributeMap attributeMap) {
            this.requestId = requestId;
            this.connectionId = connectionId;
            this.attributeMap = attributeMap;
        }

        public ReplyMessage getReply() {
            return reply;
        }

        @Override
        public void replyMessage(ReplyMessage replyMessage) {
            reply = replyMessage;
        }

        @Override
        public int getRequestId() {
            return requestId;
        }

        @Override
        public int getConnectionId() {
            return connectionId;
        }

        @Override
        public AttributeMap getAttributeMap() {
            return attributeMap;
        }
    }
}
