
package com.torodb.packaging;

import com.eightkdata.mongowp.server.wp.NettyMongoServer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.torodb.backend.guice.BackendModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.metrics.MetricsModule;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.wp.guice.MongoDbWpModule;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.guice.BackendImplementationModule;
import com.torodb.packaging.guice.ConfigModule;
import com.torodb.packaging.guice.ExecutorServicesModule;
import com.torodb.packaging.guice.PackagingModule;
import com.torodb.torod.TorodServer;
import com.torodb.torod.guice.TorodModule;
import java.time.Clock;
import javax.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
@Singleton
public class ToroDbServer extends AbstractIdleService {

    private static final Logger LOGGER = LogManager.getLogger(ToroDbServer.class);

    private final BuildProperties buildProperties;
    private final TorodServer torod;
    private final MongodServer mongod;
    private final NettyMongoServer netty;
    private final ExecutorsService executorsService;

    @Inject
    ToroDbServer(BuildProperties buildProperties, TorodServer torod, MongodServer mongod, NettyMongoServer netty,
            ExecutorsService executorsService) {
        this.buildProperties = buildProperties;
        this.torod = torod;
        this.mongod = mongod;
        this.netty = netty;
        this.executorsService = executorsService;
    }

    public static ToroDbServer create(Config config, Clock clock) throws ProvisionException {
        Injector injector = createInjector(config, clock);
        return injector.getInstance(ToroDbServer.class);
    }

    public static Injector createInjector(Config config, Clock clock) {
        Injector injector = Guice.createInjector(
                new ConfigModule(config),
                new PackagingModule(clock),
                new CoreModule(),
                new BackendImplementationModule(config),
                new BackendModule(),
                new MetainfModule(),
                new D2RModule(),
                new TorodModule(),
                new MongoLayerModule(),
                new MongoDbWpModule(),
                new MetricsModule(config.getGeneric()),
                new ExecutorServicesModule()
        );
        return injector;
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting up ToroDB v" +  buildProperties.getFullVersion());

        executorsService.startAsync();
        executorsService.awaitRunning();

        torod.startAsync();
        mongod.startAsync();

        LOGGER.debug("Waiting for Mongod to be running");
        mongod.awaitRunning();
        netty.startAsync();

        LOGGER.debug("Waiting for Torod to be running");
        torod.awaitRunning();
        LOGGER.debug("Waiting for Netty to be running");
        netty.awaitRunning();

        LOGGER.debug("ToroDbServer ready to run");
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Shutting down ToroDB");

        netty.stopAsync();
        netty.awaitTerminated();

        mongod.stopAsync();
        mongod.awaitTerminated();

        torod.stopAsync();
        torod.awaitTerminated();

        executorsService.stopAsync();
        executorsService.awaitTerminated();

        LOGGER.debug("ToroDBServer shutdown complete");
    }

}
