
package com.torodb.packaging.stampede;

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.torodb.core.backend.BackendBundle;
import com.torodb.core.backend.BackendService;
import com.torodb.d2r.guice.D2RModule;
import com.torodb.mongodb.core.MongodServer;
import com.torodb.mongodb.guice.MongoLayerModule;
import com.torodb.mongodb.repl.*;
import com.torodb.torod.TorodBundle;
import com.torodb.torod.TorodBundleFactory;
import com.torodb.torod.guice.SqlTorodModule;
import javax.inject.Singleton;

/**
 *
 */
public class StampedeRuntimeModule extends PrivateModule {

    private final BackendBundle backend;
    private final StampedeService stampedeService;
    private final ConsistencyHandler consistencyHandler;

    public StampedeRuntimeModule(BackendBundle backend,
            StampedeService stampedeService,
            ConsistencyHandler consistencyHandler) {
        this.backend = backend;
        this.stampedeService = stampedeService;
        this.consistencyHandler = consistencyHandler;
    }

    @Override
    protected void configure() {
        bind(ConsistencyHandler.class)
                .toInstance(consistencyHandler);
        expose(ConsistencyHandler.class);
        bind(BackendService.class)
                .toInstance(backend.getBackendService());
        expose(BackendService.class);
        
        install(new D2RModule());
        install(new SqlTorodModule());
        install(new MongoLayerModule());
        expose(MongodServer.class);
    }

    @Provides @Singleton @Exposed
    TorodBundle createTorodBundle(TorodBundleFactory factory) {
        return factory.createBundle(stampedeService, backend);
    }

}
