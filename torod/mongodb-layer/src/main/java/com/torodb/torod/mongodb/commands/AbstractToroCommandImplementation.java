
package com.torodb.torod.mongodb.commands;

import com.eightkdata.mongowp.mongoserver.api.safe.CommandImplementation;
import com.eightkdata.mongowp.mongoserver.api.safe.CommandRequest;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.mongodb.RequestContext;
import javax.annotation.Nonnull;

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
