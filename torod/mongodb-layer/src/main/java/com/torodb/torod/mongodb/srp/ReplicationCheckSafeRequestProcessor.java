
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.mongoserver.api.safe.*;
import com.eightkdata.mongowp.mongoserver.api.safe.impl.UpdateOpResult;
import com.eightkdata.mongowp.mongoserver.api.safe.pojos.QueryRequest;
import com.eightkdata.mongowp.mongoserver.callback.WriteOpResult;
import com.eightkdata.mongowp.mongoserver.protocol.exceptions.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import com.torodb.torod.mongodb.repl.ReplInterface.MemberStateInterface;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@linkplain SafeRequestProcessor} enforces the replication restriction
 * policies on each reaquest.
 *
 * If the replication restrictions are fulfilled, then the request is redirected
 * to the deeper delegator. If it is not, then a {@linkplain MongoException} is
 * thrown.
 */
@Singleton
public final class ReplicationCheckSafeRequestProcessor extends DecoratorSafeRequestProcessor {
    private static final Logger LOGGER
            = LoggerFactory.getLogger(ReplicationCheckSafeRequestProcessor.class);
    
    private final ReplCoordinator replCoord;

    @Inject
    public ReplicationCheckSafeRequestProcessor(ToroSafeRequestProcessor delegate, ReplCoordinator replCoordinator) {
        super(delegate);
        this.replCoord = replCoordinator;
    }

    @Override
    public ReplyMessage getMore(Request request, GetMoreMessage getMoreMessage)
            throws MongoException {
        MemberStateInterface replState = replCoord.freezeMemberState(false);
        try {
            if (!replState.canNodeAcceptReads(getMoreMessage.getDatabase())) {
                LOGGER.debug("ReplCheck: GetMore on {} refused", getMoreMessage.getDatabase());
                throw new NotMasterOrSecondaryException();
            }
            LOGGER.trace("ReplCheck: GetMore on {} accepted", getMoreMessage.getDatabase());
            return getDelegate().getMore(request, getMoreMessage);
        } finally {
            replState.close();
        }
    }

    @Override
    public ListenableFuture<?> killCursors(Request request, KillCursorsMessage killCursorsMessage)
            throws MongoException {
        LOGGER.trace("ReplCheck: KillCursors accepted");
        return getDelegate().killCursors(request, killCursorsMessage);
    }

    @Override
    public <Arg, Result> CommandReply<Result> execute(
            Command<? super Arg, ? super Result> command,
            CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        MemberStateInterface replState = replCoord.freezeMemberState(command.canChangeReplicationState());
        try {
            switch (replState.getMemberState()) {
                case RS_PRIMARY: {
                    break;
                }
                case RS_SECONDARY: {
                    if (command.isSlaveOk()) {
                        break;
                    }
                    if (command.isSlaveOverrideOk()) {
                        if (request.isSlaveOk()) {
                            break;
                        }
                        LOGGER.debug("ReplCheck: Command '{}' refused because not isSlaveOk request", command.getCommandName());
                        throw new NotMasterNoSlaveOkCodeException();
                    }
                    else {
                        throw new NotMasterException();
                    }
                }
                default: {
                    //TODO: Check implementation
                    LOGGER.debug("ReplCheck: Command '{}' refused because not master or secondary state", command.getCommandName());
                    throw new NotMasterOrSecondaryException();
                }
            }
            LOGGER.trace("ReplCheck: Command '{}' accepted", command.getCommandName());
            return getDelegate().execute(command, request);
        } finally {
            replState.close();
        }
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest queryMessage) throws
            MongoException {
        MemberStateInterface replState = replCoord.freezeMemberState(false);
        try {
            if (!replState.canNodeAcceptReads(request.getDatabase())) {
                LOGGER.debug("ReplCheck: Query on {} refused", request.getDatabase());
                throw new NotMasterOrSecondaryException();
            }
            LOGGER.trace("ReplCheck: Query on {} accepted", request.getDatabase());
            return getDelegate().query(request, queryMessage);
        } finally {
            replState.close();
        }
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        MemberStateInterface replState = replCoord.freezeMemberState(false);
        try {
            if (!replState.canNodeAcceptWrites(request.getDatabase())) {
                LOGGER.debug("ReplCheck: Insert on {} refused", request.getDatabase());
                throw new NotMasterException();
            }
            LOGGER.trace("ReplCheck: Insert on {} accepted", request.getDatabase());
            return getDelegate().insert(request, insertMessage);
        } finally {
            replState.close();
        }
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage deleteMessage)
            throws MongoException {
        MemberStateInterface replState = replCoord.freezeMemberState(false);
        try {
            if (!replState.canNodeAcceptWrites(request.getDatabase())) {
                LOGGER.debug("ReplCheck: Update on {} refused", request.getDatabase());
                throw new NotMasterException();
            }

            LOGGER.trace("ReplCheck: Update on {} accepted", request.getDatabase());
            return getDelegate().update(request, deleteMessage);
        } finally {
            replState.close();
        }
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        MemberStateInterface replState = replCoord.freezeMemberState(false);
        try {
            if (!replState.canNodeAcceptWrites(request.getDatabase())) {
                LOGGER.debug("ReplCheck: Delete on {} refused", request.getDatabase());
                throw new NotMasterException();
            }

            LOGGER.trace("ReplCheck: Delete on {} accepted", request.getDatabase());
            return getDelegate().delete(request, deleteMessage);
        } finally {
            replState.close();
        }
    }
}
