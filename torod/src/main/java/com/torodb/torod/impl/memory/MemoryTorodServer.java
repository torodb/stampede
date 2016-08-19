
package com.torodb.torod.impl.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.torod.TorodConnection;
import com.torodb.torod.TorodServer;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

/**
 *
 */
public class MemoryTorodServer  extends ThreadFactoryIdleService implements TorodServer {

    private final MemoryData data = new MemoryData();
    private final AtomicInteger connIdGenerator = new AtomicInteger();
    private final Cache<Integer, MemoryTorodConnection> openConnections;

    @Inject
    public MemoryTorodServer(ThreadFactory threadFactory) {
        super(threadFactory);

        openConnections = CacheBuilder.newBuilder()
                .weakValues()
                .removalListener(this::onConnectionInvalidated)
                .build();
    }

    @Override
    public TorodConnection openConnection() {
        return new MemoryTorodConnection(this, connIdGenerator.incrementAndGet());
    }

    @Override
    public void disableDataImportMode() {
    }

    @Override
    public void enableDataImportMode() {
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        openConnections.invalidateAll();
        try (MemoryData.MDWriteTransaction trans = data.openWriteTransaction()) {
            trans.clear();
        }
    }

    MemoryData getData() {
        return data;
    }

    private void onConnectionInvalidated(RemovalNotification<Integer, MemoryTorodConnection> notification) {
        MemoryTorodConnection value = notification.getValue();
        if (value != null) {
            value.close();
        }
    }

    void onConnectionClosed(MemoryTorodConnection connection) {
        openConnections.invalidate(connection.getConnectionId());
    }

}
