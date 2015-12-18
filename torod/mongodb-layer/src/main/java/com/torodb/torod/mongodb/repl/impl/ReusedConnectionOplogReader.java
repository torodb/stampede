
package com.torodb.torod.mongodb.repl.impl;

import com.eightkdata.mongowp.client.core.MongoConnection;
import com.google.common.net.HostAndPort;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ReusedConnectionOplogReader extends AbstractMongoOplogReader {

    private boolean closed;
    /**
     * Not owned, meaning that is not closed when this reader is closed
     */
    private final MongoConnection connection;

    /**
     *
     * @param connection The connection that will be used. It won't be closed when calling {@linkplain #close() }
     */
    ReusedConnectionOplogReader(@Nonnull MongoConnection connection) {
        this.connection = connection;
        this.closed = false;
    }

    @Override
    public HostAndPort getSyncSource() {
        return connection.getClientOwner().getAddress();
    }

    @Override
    protected MongoConnection consumeConnection() {
        return connection;
    }

    @Override
    protected void releaseConnection(MongoConnection connection) {
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
