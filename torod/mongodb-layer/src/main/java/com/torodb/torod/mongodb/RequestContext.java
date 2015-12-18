
package com.torodb.torod.mongodb;

import com.eightkdata.mongowp.mongoserver.api.safe.Request;
import com.torodb.torod.core.connection.ToroConnection;
import com.torodb.torod.mongodb.repl.ReplCoordinator;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import javax.annotation.Nonnull;

/**
 *
 */
public class RequestContext {
    private final static AttributeKey<RequestContext> USED_ATTRIBUTE =
			AttributeKey.valueOf(RequestContext.class.getCanonicalName());

    private final @Nonnull String supportedDatabase;
    private final @Nonnull ToroConnection toroConnection;
    private final @Nonnull ReplCoordinator replicationCoordinator;
    private final @Nonnull OptimeClock optimeClock;

    public RequestContext(
            String supportedDatabase,
            ToroConnection toroConnection,
            ReplCoordinator replicationCoordinator,
            OptimeClock optimeClock) {
        this.supportedDatabase = supportedDatabase;
        this.toroConnection = toroConnection;
        this.replicationCoordinator = replicationCoordinator;
        this.optimeClock = optimeClock;
    }

    public void setTo(AttributeMap attMap) {
        attMap.attr(USED_ATTRIBUTE).set(this);
    }
    
    public static RequestContext getAndRemoveFrom(AttributeMap attMap) {
        return attMap.attr(USED_ATTRIBUTE).getAndRemove();
    }

    public static RequestContext getFrom(AttributeMap attMap) {
        return attMap.attr(USED_ATTRIBUTE).get();
    }
    
    public static RequestContext getFrom(Request req) {
        return RequestContext.getFrom(req.getConnection().getAttributeMap());
    }

    public String getSupportedDatabase() {
        return supportedDatabase;
    }

    public ToroConnection getToroConnection() {
        return toroConnection;
    }

    public ReplCoordinator getReplicationCoordinator() {
        return replicationCoordinator;
    }

    public OptimeClock getOptimeClock() {
        return optimeClock;
    }
}
