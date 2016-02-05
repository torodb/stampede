
package com.torodb.torod.mongodb.srp;

import com.eightkdata.mongowp.server.api.SafeRequestProcessor;
import com.eightkdata.mongowp.server.api.CommandsLibrary;
import com.eightkdata.mongowp.server.api.Connection;
import com.eightkdata.mongowp.server.api.CommandReply;
import com.eightkdata.mongowp.server.api.Command;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.eightkdata.mongowp.server.api.Request;
import com.eightkdata.mongowp.exceptions.CommandNotSupportedException;
import com.eightkdata.mongowp.exceptions.MongoException;
import com.eightkdata.mongowp.messages.request.*;
import com.eightkdata.mongowp.messages.response.ReplyMessage;
import com.eightkdata.mongowp.server.api.impl.UpdateOpResult;
import com.eightkdata.mongowp.server.api.pojos.QueryRequest;
import com.eightkdata.mongowp.server.callback.WriteOpResult;
import com.google.common.util.concurrent.ListenableFuture;
import javax.inject.Inject;

/**
 *
 */
class DecoratorSafeRequestProcessor implements SafeRequestProcessor {

    private final SafeRequestProcessor delegate;

    @Inject
    public DecoratorSafeRequestProcessor(SafeRequestProcessor delegate) {
        this.delegate = delegate;
    }

    SafeRequestProcessor getDelegate() {
        return delegate;
    }

    @Override
    public void onConnectionActive(Connection connection) {
        delegate.onConnectionActive(connection);
    }

    @Override
    public void onConnectionInactive(Connection connection) {
        delegate.onConnectionInactive(connection);
    }

    @Override
    public ReplyMessage getMore(Request request, GetMoreMessage getMoreMessage)
            throws MongoException {
        return delegate.getMore(request, getMoreMessage);
    }

    @Override
    public ListenableFuture<?> killCursors(Request request, KillCursorsMessage killCursorsMessage)
            throws MongoException {
        return delegate.killCursors(request, killCursorsMessage);
    }

    @Override
    public CommandsLibrary getCommandsLibrary() {
        return delegate.getCommandsLibrary();
    }

    @Override
    public <Arg, Result> CommandReply<Result> execute(
            Command<? super Arg, ? super Result> command,
            CommandRequest<Arg> request)
            throws MongoException, CommandNotSupportedException {
        return delegate.execute(command, request);
    }

    @Override
    public ReplyMessage query(Request request, QueryRequest queryMessage) throws
            MongoException {
        return delegate.query(request, queryMessage);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> insert(Request request, InsertMessage insertMessage)
            throws MongoException {
        return delegate.insert(request, insertMessage);
    }

    @Override
    public ListenableFuture<? extends UpdateOpResult> update(Request request, UpdateMessage updateMessage)
            throws MongoException {
        return delegate.update(request, updateMessage);
    }

    @Override
    public ListenableFuture<? extends WriteOpResult> delete(Request request, DeleteMessage deleteMessage)
            throws MongoException {
        return delegate.delete(request, deleteMessage);
    }

}
