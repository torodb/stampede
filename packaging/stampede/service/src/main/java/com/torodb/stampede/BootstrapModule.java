
package com.torodb.stampede;

import java.time.Clock;

import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.torodb.concurrent.guice.ConcurrentModule;
import com.torodb.core.BuildProperties;
import com.torodb.core.guice.CoreModule;
import com.torodb.core.metrics.guice.MetricsModule;
import com.torodb.metainfo.guice.MetainfModule;
import com.torodb.mongodb.core.MongodServerConfig;
import com.torodb.packaging.DefaultBuildProperties;
import com.torodb.packaging.guice.BackendImplementationModule;
import com.torodb.packaging.guice.ExecutorServicesModule;
import com.torodb.packaging.guice.PackagingModule;
import com.torodb.stampede.config.model.Config;

/**
 *
 */
class BootstrapModule extends AbstractModule {

    private final Config config;
    private final Clock clock;
    
    public BootstrapModule(Config config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    @Override
    protected void configure() {
        binder().requireExplicitBindings();
        install(new PackagingModule(clock));
        install(new CoreModule());
        install(new ExecutorServicesModule());
        install(new ConcurrentModule());
        install(new MetainfModule());
        install(new MetricsModule(config));

        install(new BackendImplementationModule(
                config.getReplication(),
                config.getBackend().getPool(),
                config.getBackend().getBackendImplementation()
        ));
        bind(Config.class)
                .toInstance(config);
        bind(MongodServerConfig.class)
                .toInstance(new MongodServerConfig(HostAndPort.fromParts("localhost", 27017)));
        bind(BuildProperties.class)
                .to(DefaultBuildProperties.class)
                .asEagerSingleton();
    }
    
}
