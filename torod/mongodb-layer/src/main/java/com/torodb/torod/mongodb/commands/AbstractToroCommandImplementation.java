
package com.torodb.torod.mongodb.commands;

import javax.annotation.Nonnull;

import com.eightkdata.mongowp.server.api.CommandImplementation;
import com.eightkdata.mongowp.server.api.CommandRequest;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.mongodb.RequestContext;

/**
 *
 */
public abstract class AbstractToroCommandImplementation<Arg, Rep> implements CommandImplementation<Arg, Rep> {

    @Nonnull
    protected final RequestContext getRequestContext(CommandRequest<Arg> req) {
        return RequestContext.getFrom(req);
    }

    @Nonnull
    protected final ToroConnection getToroConnection(CommandRequest<Arg> req) {
        return RequestContext.getFrom(req).getToroConnection();
    }

}
