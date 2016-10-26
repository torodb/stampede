
package com.torodb.stampede;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.eightkdata.mongowp.client.wrapper.MongoClientConfiguration;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.torodb.core.Shutdowner;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendBundleFactory;
import com.torodb.core.backend.BackendConnection;
import com.torodb.core.backend.BackendService;
import com.torodb.core.backend.ExclusiveWriteBackendTransaction;
import com.torodb.core.exceptions.user.UserException;
import com.torodb.core.retrier.Retrier;
import com.torodb.core.supervision.Supervisor;
import com.torodb.core.supervision.SupervisorDecision;
import com.torodb.mongodb.repl.ConsistencyHandler;
import com.torodb.mongodb.repl.MongodbReplBundle;
import com.torodb.mongodb.repl.ReplicationFilters;
import com.torodb.mongodb.repl.guice.MongodbReplConfig;
import com.torodb.packaging.config.model.protocol.mongo.Replication;
import com.torodb.packaging.util.MongoClientConfigurationFactory;
import com.torodb.packaging.util.ReplicationFiltersFactory;
import com.torodb.stampede.config.model.Config;
import com.torodb.torod.TorodBundle;

/**
 *
 */
public class StampedeService extends AbstractIdleService implements Supervisor {

    private static final Logger LOGGER
            = LogManager.getLogger(StampedeService.class);
    private final ThreadFactory threadFactory;
    private final Injector bootstrapInjector;
    private Shutdowner shutdowner;

    public StampedeService(ThreadFactory threadFactory, Injector bootstrapInjector) {
        this.threadFactory = threadFactory;
        this.bootstrapInjector = bootstrapInjector;
    }

    @Override
    protected Executor executor() {
        return (Runnable command) -> {
            Thread thread = threadFactory.newThread(command);
            thread.start();
        };
    }

    @Override
    public SupervisorDecision onError(Object supervised, Throwable error) {
        this.stopAsync();
        return SupervisorDecision.STOP;
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting up ToroDB Stampede");

        shutdowner = bootstrapInjector.getInstance(Shutdowner.class);

        BackendBundle backendBundle = createBackendBundle();
        startBundle(backendBundle);

        BackendService backendService = backendBundle.getBackendService();

        ConsistencyHandler consistencyHandler = createConsistencyHandler(
                backendService);
        if (!consistencyHandler.isConsistent()) {
            LOGGER.info("Database is not consistent. Cleaning it up");
            dropDatabase(backendService);
        }

        Injector finalInjector = createFinalInjector(
                backendBundle, consistencyHandler);

        Replication replication = getReplication();
        reportReplication(replication);
        TorodBundle torodBundle = createTorodBundle(finalInjector);
        startBundle(torodBundle);

        MongodbReplConfig replConfig = getReplConfig(replication);

        startBundle(createMongodbReplBundle(finalInjector, torodBundle, replConfig));

        LOGGER.info("ToroDB Stampede is now running");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Shutting down ToroDB Stampede");
        if (shutdowner != null) {
            shutdowner.close();
        }
        LOGGER.info("ToroDB Stampede has been shutted down");
    }

    private BackendBundle createBackendBundle() {
        return bootstrapInjector.getInstance(BackendBundleFactory.class)
                        .createBundle(this);
    }

    private ConsistencyHandler createConsistencyHandler(BackendService backendService) {
        Retrier retrier = bootstrapInjector.getInstance(Retrier.class);
        return new DefaultConsistencyHandler(backendService, retrier);
    }

    private void dropDatabase(BackendService backendService) throws UserException {
        try (BackendConnection conn = backendService.openConnection();
                ExclusiveWriteBackendTransaction trans = conn.openExclusiveWriteTransaction()) {
            trans.dropUserData();
            trans.commit();
        }
    }

    private Injector createFinalInjector(BackendBundle backendBundle,
            ConsistencyHandler consistencyHandler) {
        StampedeRuntimeModule runtimeModule = new StampedeRuntimeModule(
                backendBundle, this, consistencyHandler);
        return bootstrapInjector.createChildInjector(runtimeModule);
    }

    private MongodbReplBundle createMongodbReplBundle(Injector finalInjector,
            TorodBundle torodBundle, MongodbReplConfig replConfig) {
        return new MongodbReplBundle(torodBundle, this, replConfig, finalInjector);
    }

    private TorodBundle createTorodBundle(Injector finalInjector) {
        return finalInjector.getInstance(TorodBundle.class);
    }

    private void startBundle(Service service) {
        service.startAsync();
        service.awaitRunning();

        shutdowner.addStopShutdownListener(service);
    }

    private Replication getReplication() {
        Config config = bootstrapInjector.getInstance(Config.class);
        return config.getReplication();
    }

    private void reportReplication(Replication replication) {
        LOGGER.info("Replicating from seeds: {}", replication.getSyncSource());
    }

    private MongodbReplConfig getReplConfig(Replication replication) {
        return new DefaultMongodbReplConfig(replication);
    }

    private static class DefaultMongodbReplConfig implements MongodbReplConfig {
        private final MongoClientConfiguration mongoClientConf;
        private final ReplicationFilters replFilters;
        private final String replSetName;

        public DefaultMongodbReplConfig(Replication replication) {
            this.mongoClientConf = MongoClientConfigurationFactory.getMongoClientConfiguration(replication);
            this.replFilters = ReplicationFiltersFactory.getReplicationFilters(replication);
            replSetName = replication.getReplSetName();
        }

        @Override
        public MongoClientConfiguration getMongoClientConfiguration() {
            return mongoClientConf;
        }

        @Override
        public ReplicationFilters getReplicationFilters() {
            return replFilters;
        }

        @Override
        public String getReplSetName() {
            return replSetName;
        }
    }

}