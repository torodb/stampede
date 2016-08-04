
package com.torodb.torod;

import akka.actor.ActorSystem;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.torodb.common.util.ThreadFactoryIdleService;
import com.torodb.core.TableRefFactory;
import com.torodb.core.annotations.ToroDbIdleService;
import com.torodb.core.backend.Backend;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.IdentifierFactory;
import com.torodb.core.transaction.InternalTransactionManager;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import com.torodb.torod.guice.TorodLayer;
import com.torodb.torod.pipeline.InsertPipelineFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;
import javax.inject.Singleton;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

/**
 *
 */
@Singleton
public class TorodServer extends ThreadFactoryIdleService {

    private final AtomicInteger connectionIdCounter = new AtomicInteger();
    private final D2RTranslatorFactory d2RTranslatorFactory;
    private final IdentifierFactory idFactory;
    private final InsertPipelineFactory insertPipelineFactory;
    private final Cache<Integer, TorodConnection> openConnections;
    private final Backend backend;
    private final InternalTransactionManager internalTransactionManager;
    private final TableRefFactory tableRefFactory;
    private final ActorSystem actorSystem;

    @Inject
    public TorodServer(@ToroDbIdleService ThreadFactory threadFactory,
            D2RTranslatorFactory d2RTranslatorFactory, IdentifierFactory idFactory,
            InsertPipelineFactory insertPipelineFactory, Backend backend, 
            InternalTransactionManager internalTransactionManager, TableRefFactory tableRefFactory,
            @TorodLayer ActorSystem actorSystem) {
        super(threadFactory);
        this.d2RTranslatorFactory = d2RTranslatorFactory;
        this.idFactory = idFactory;
        this.insertPipelineFactory = insertPipelineFactory;
        this.backend = backend;
        this.internalTransactionManager = internalTransactionManager;
        this.tableRefFactory = tableRefFactory;
        this.actorSystem = actorSystem;
        
        openConnections = CacheBuilder.newBuilder()
                .weakValues()
                .removalListener(this::onConnectionInvalidated)
                .build();
    }

    public TorodConnection openConnection() {
        int connectionId = connectionIdCounter.incrementAndGet();
        TorodConnection connection = new TorodConnection(this, connectionId);
        openConnections.put(connectionId, connection);

        return connection;
    }

    @Override
    protected void startUp() throws Exception {
        backend.startAsync();
        backend.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        openConnections.invalidateAll();
        backend.stopAsync();
        backend.awaitTerminated();
        Await.result(actorSystem.terminate(), Duration.Inf());
    }

    private void onConnectionInvalidated(RemovalNotification<Integer, TorodConnection> notification) {
        TorodConnection value = notification.getValue();
        if (value != null) {
            value.close();
        }
    }

    public void disableInternalIndexes() {
        ImmutableMetaSnapshot snapshot = internalTransactionManager.takeMetaSnapshot();
        backend.disableInternalIndexes(snapshot);
    }

    public void enableInternalIndexes() {
        ImmutableMetaSnapshot snapshot = internalTransactionManager.takeMetaSnapshot();
        backend.enableInternalIndexes(snapshot);
    }

    D2RTranslatorFactory getD2RTranslatorrFactory() {
        return d2RTranslatorFactory;
    }

    IdentifierFactory getIdentifierFactory() {
        return idFactory;
    }

    InsertPipelineFactory getInsertPipelineFactory() {
        return insertPipelineFactory;
    }

    Backend getBackend() {
        return backend;
    }

    InternalTransactionManager getInternalTransactionManager() {
        return internalTransactionManager;
    }

    void onConnectionClosed(TorodConnection connection) {
        openConnections.invalidate(connection.getConnectionId());
    }

    TableRefFactory getTableRefFactory() {
        return tableRefFactory;
    }

}
