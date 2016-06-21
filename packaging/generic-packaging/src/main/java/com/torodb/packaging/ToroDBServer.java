
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
import com.torodb.d2r.guice.D2RModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.packaging.config.model.Config;
import com.torodb.packaging.guice.BackendImplementationModule;
import com.torodb.packaging.guice.ConfigModule;
import com.torodb.packaging.guice.PackagingModule;
import com.torodb.torod.TorodServer;
import com.torodb.torod.guice.TorodModule;
import java.time.Clock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 */
public class ToroDBServer extends AbstractIdleService {

    private static final Logger LOGGER = LogManager.getLogger(ToroDBServer.class);

    private final BuildProperties buildProperties;
    private final TorodServer torod;
    private final MongodServer mongod;
    private final NettyMongoServer netty;

    @Inject
    ToroDBServer(BuildProperties buildProperties, TorodServer torod, MongodServer mongod, NettyMongoServer netty) {
        this.buildProperties = buildProperties;
        this.torod = torod;
        this.mongod = mongod;
        this.netty = netty;
    }

    public static ToroDBServer create(Config config, Clock clock) throws ProvisionException {
        Injector injector = Guice.createInjector(
                new ConfigModule(config),
                new PackagingModule(clock),
                new CoreModule(),
                new BackendImplementationModule(config),
                new BackendModule(),
                new MetainfModule(),
                new D2RModule(),
                new TorodModule(),
                new MongoLayerModule()
        );
        return injector.getInstance(ToroDBServer.class);
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Starting up ToroDB v" +  buildProperties.getFullVersion());

        torod.startAsync();
        mongod.startAsync();

        mongod.awaitRunning();
        netty.startAsync();

        torod.awaitRunning();
        netty.awaitRunning();
    }

    @Override
    protected void shutDown() throws Exception {
        LOGGER.info("Shutting up ToroDB");

        netty.stopAsync();
        netty.awaitTerminated();

        mongod.stopAsync();
        mongod.awaitTerminated();

        torod.stopAsync();
        torod.awaitTerminated();
    }

}
